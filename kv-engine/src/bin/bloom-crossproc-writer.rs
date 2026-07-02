use kv_engine::lsm_storage::{KvEngine, LsmStorageOptions};

fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime")
        .block_on(future)
}

fn block_on_result<T, E>(future: impl std::future::Future<Output = Result<T, E>>) -> Result<T, E> {
    block_on(future)
}

fn main() {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: bloom-crossproc-writer <db-path>");
        std::process::exit(2);
    };

    let mut opts = LsmStorageOptions::default_for_test();
    opts.enable_wal = true;
    opts.manifest_snapshot_threshold_bytes = 256;

    let engine = block_on_result(KvEngine::open(path, opts)).expect("open writer db");
    for i in 0..100u32 {
        let key = format!("k_{i:010}");
        let value = if i % 2 == 0 {
            format!("v_{i}")
        } else {
            "y".repeat(1000)
        };
        block_on_result(engine.put(key.as_bytes(), value.as_bytes())).expect("put");
        if i % 10 == 9 {
            block_on_result(engine.force_flush()).expect("force_flush");
        }
    }
    block_on_result(engine.close()).expect("close writer db");
}
