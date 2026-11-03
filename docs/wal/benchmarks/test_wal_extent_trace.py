"""Extent ownership, frame identity, temporal evidence and calibration boundaries."""

import copy
import struct
import tempfile
import unittest
from unittest.mock import patch
import zlib

import wal_extent_trace as ext


DEVICE = {"fs_major": 259, "fs_minor": 3, "disk_major": 259, "disk_minor": 0,
          "partition_start_bytes": 4096, "logical_block_bytes": 512, "nsid": 1, "disk": "nvme0n1"}


def timeline():
    file = {"path": "pitr-1.wal", "inode": 55, "dev_major": 259, "dev_minor": 3,
            "fs_block_bytes": 4096, "version": 8, "header_sha256": 'ab' * 32,
            "snapshot_stable": True, "size": 16384,
            "extents": [{"logical": 0, "physical": 81920, "length": 16384, "flags": 1}],
            "frames": [{"start": 0, "end": 4096, "kind": "HEADER"},
                       {"start": 4096, "end": 8192, "kind": "FRONTIER"},
                       {"start": 8192, "end": 12288, "kind": "DATA"},
                       {"start": 12288, "end": 16384, "kind": "FRONTIER"}]}
    old = copy.deepcopy(file)
    old['size'] = 8192
    old['extents'][0]['length'] = 8192
    old['frames'] = []
    return {'process_id': 100, 'counter_points': [{'sample_begin_ns': 10, 'sample_end_ns': 11},
                                                {'sample_begin_ns': 99, 'sample_end_ns': 100}],
            'wal_extents': {'before': {'sample_begin_ns': 1, 'sample_end_ns': 2, 'fiemap_flags': 0,
                                       'clock': 'CLOCK_MONOTONIC', 'files': [old]},
                            'after': {'sample_begin_ns': 101, 'sample_end_ns': 105, 'fiemap_flags': 0,
                                      'clock': 'CLOCK_MONOTONIC', 'files': [file]}}}


def map_event(at=5, pblk=22):
    return {'event': 'ext4/ext4_ext_map_blocks_exit', 'dev': 259 << 20 | 3, 'ino': 55,
            'time_ns': at, 'lblk': 2, 'pblk': pblk, 'ret': 2, 'len': 2}


def command(begin=20, end=30, location=94208, size=4096, role='DATA', opcode=1):
    return {'key': [0, 1, begin], 'start_ns': begin, 'end_ns': end, 'opcode': opcode,
            'bytes': size, 'pid': 100, 'tid': 101, 'nsid': 1, 'status': 0, 'retries': 0,
            'control': 0, 'wal_role': role, 'wal_file': [259, 3, 55], 'logical_start': 8192,
            'location_for_test': location}


def setups(commands):
    return [{'event': 'nvme/nvme_setup_cmd', 'time_ns': c['start_ns'], 'disk': 'nvme0n1',
             'ctrl_id': c['key'][0], 'qid': c['key'][1], 'cid': c['key'][2],
             'lba': c['location_for_test'] // 512} for c in commands]


class FiemapTests(unittest.TestCase):
    def test_ioctl_does_not_request_sync_and_preserves_unwritten(self):
        def ioctl(_fd, request, buf, mutate):
            self.assertEqual(request, ext.FS_IOC_FIEMAP)
            self.assertEqual(struct.unpack_from('=I', buf, 16)[0], 0)
            self.assertTrue(mutate)
            struct.pack_into('=I', buf, 20, 1)
            struct.pack_into('=QQQQQI', buf, 32, 0, 81920, 4096, 0, 0, 0x801)
        with patch.object(ext.fcntl, 'ioctl', ioctl):
            self.assertEqual(ext.fiemap(7), [{'logical': 0, 'physical': 81920, 'length': 4096, 'flags': 0x801}])

    def test_unknown_delalloc_shared_or_encoded_extents_are_not_used(self):
        for flags in (2, 4, 8, 0x80, 0x100, 0x2000):
            self.assertEqual(list(ext.mapped_blocks([{'logical': 0, 'physical': 81920, 'length': 4096, 'flags': flags}])), [])

    def test_frame_inventory_checks_crc_offset_header_binding_and_kind(self):
        digest = bytes.fromhex('ab' * 32)
        frame = bytearray(4096)
        frame[:8] = b'TKVW8FR1'
        struct.pack_into('>HHHH', frame, 8, 1, 1, 64, 0)
        frame[16:48] = digest
        struct.pack_into('>QI', frame, 48, 8192, 100)
        struct.pack_into('>I', frame, 60, zlib.crc32(frame[64:], zlib.crc32(frame[:60])))
        self.assertEqual(ext.frame_kind(frame, 8192, 8, digest), 'DATA')
        self.assertEqual(ext.frame_kind(frame, 12288, 8, digest), 'unknown')
        self.assertEqual(ext.frame_kind(frame, 8192, 8, b'x' * 32), 'unknown')
        frame[100] ^= 1
        self.assertEqual(ext.frame_kind(frame, 8192, 8, digest), 'unknown')


class AttributionTests(unittest.TestCase):
    def attribute(self, commands, events=None, tl=None):
        info = ext.attribute_commands(commands, setups(commands) + (events or []), tl or timeline(), DEVICE)
        return info, [c['wal_role'] for c in commands]

    def test_partition_offset_and_inode_mapping_identify_data(self):
        info, roles = self.attribute([command()], [map_event()])
        self.assertEqual(roles, ['DATA'])
        self.assertEqual(info['mapping_problems'], {})

    def test_post_only_map_is_not_retroactively_used(self):
        _, roles = self.attribute([command()])
        self.assertEqual(roles, ['UNOBSERVED_ALLOCATION'])

    def test_late_mapping_event_cannot_authorize_earlier_io(self):
        _, roles = self.attribute([command()], [map_event(at=40)])
        self.assertEqual(roles, ['UNOBSERVED_ALLOCATION'])

    def test_initially_allocated_unwritten_range_is_valid_evidence(self):
        tl = timeline()
        tl['wal_extents']['before']['files'][0]['extents'][0].update(length=16384, flags=0x801)
        _, roles = self.attribute([command()], tl=tl)
        self.assertEqual(roles, ['DATA'])

    def test_remapped_physical_blocks_fail_closed(self):
        _, roles = self.attribute([command()], [map_event(), map_event(at=15, pblk=99)])
        self.assertEqual(roles, ['MAPPING_CONFLICT'])

    def test_metadata_free_does_not_invalidate_data_but_data_free_does(self):
        free = {'event': 'ext4/ext4_free_blocks', 'dev': 259 << 20 | 3, 'ino': 55, 'time_ns': 15, 'flags': 1}
        _, roles = self.attribute([command()], [map_event(), free])
        self.assertEqual(roles, ['DATA'])
        _, roles = self.attribute([command()], [map_event(), {**free, 'flags': 0}])
        self.assertEqual(roles, ['MAPPING_CONFLICT'])

    def test_initialization_write_is_not_called_data(self):
        _, roles = self.attribute([command(begin=12, end=15), command()], [map_event()])
        self.assertEqual(roles, ['OVERWRITTEN_OR_INITIALIZATION', 'DATA'])

    def test_same_sector_concurrent_writes_are_ambiguous(self):
        _, roles = self.attribute([command(begin=12, end=25), command()], [map_event()])
        self.assertEqual(roles, ['MAPPING_CONFLICT', 'MAPPING_CONFLICT'])

    def test_partial_sector_overwrite_invalidates_previous_full_frame(self):
        partial = command(begin=40, end=50, location=94208 + 512, size=512)
        _, roles = self.attribute([command(), partial], [map_event()])
        self.assertEqual(roles, ['OVERWRITTEN_OR_INITIALIZATION', 'PARTIAL_EXTENT'])

    def test_frontier_and_mixed_frame_ranges_stay_distinct(self):
        _, roles = self.attribute([command(location=98304)], [map_event()])
        self.assertEqual(roles, ['FRONTIER'])
        _, roles = self.attribute([command(size=8192)], [map_event()])
        self.assertEqual(roles, ['MIXED_ROLES'])

    def test_journal_write_outside_wal_extent_stays_non_wal(self):
        write = command(location=409600)
        write.update(pid=200, control=0x4000)
        _, roles = self.attribute([write], [map_event()])
        self.assertEqual(roles, ['NON_WAL'])


class SampleTests(unittest.TestCase):
    def test_intervening_journal_fua_is_recorded_without_replacing_data(self):
        flush = command(10, 20, opcode=0)
        journal = command(25, 30, location=409600, role='NON_WAL')
        journal['control'] = 0x4000
        data = command(35, 65)
        rows = ext.wal_data_samples([flush, journal, data], DEVICE)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['write_start_ns'], 35)
        self.assertEqual(rows[0]['intervening_fua'], 1)
        self.assertEqual(rows[0]['shape']['intervening_writes'], 'fua')
        self.assertEqual(rows[0]['first_namespace_write_role'], 'NON_WAL')
        self.assertEqual(rows[0]['first_namespace_write_control'], 0x4000)
        self.assertTrue(rows[0]['eligible'])

    def test_mixed_or_unresolved_wal_write_is_not_skipped_for_later_data(self):
        commands = [command(10, 20, opcode=0), command(25, 30, role='MIXED_ROLES'), command(35, 65)]
        rows = ext.wal_data_samples(commands, DEVICE)
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]['eligible'])

    def test_overlap_and_write_size_are_separate_calibration_classes(self):
        flush = command(10, 20, opcode=0)
        data = command(25, 65, size=69632)
        read = command(30, 50, opcode=2)
        row = ext.wal_data_samples([flush, data, read], DEVICE)[0]
        self.assertEqual(row['shape']['size'], 'le_131072')
        self.assertEqual(row['shape']['queue'], '0')
        self.assertEqual(row['shape']['peak'], '1')
        self.assertTrue(row['eligible'])

    def test_descriptive_calibration_does_not_inherit_4k_threshold(self):
        samples = ext.wal_data_samples([command(10, 20, opcode=0), command(25, 65, size=69632)], DEVICE)
        row = ext.calibration_rows(samples)[0]
        self.assertFalse(row['calibrated'])
        self.assertNotIn('state', row)
        self.assertIsNone(row['p99_us'])


if __name__ == '__main__':
    unittest.main()
