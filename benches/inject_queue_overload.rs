use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use std::fs::File;
use std::io::Write;
use tokio_metrics::RuntimeMetrics;
use tokio::runtime;
use tokio::time::sleep;

const NUM_WORKERS: usize = 4;
const NUM_SPAWN: usize = 10_000;
const STALL_DUR: Duration = Duration::from_micros(10);

fn rt_multi_spawn_many_local_injectq_problem(c: &mut Criterion) {
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    // let rt = rt();

    let (tx, rx) = mpsc::sync_channel(1000);
    let rem = Arc::new(AtomicUsize::new(0));

    const NUM_SPAWN: usize = 1_000_000;

    c.bench_function("spawn_many_local_injectq", |b| {
        b.iter(|| {
            rem.store(NUM_SPAWN, Relaxed);

            rt.block_on(async {
                for _ in 0..NUM_SPAWN {
                    let tx = tx.clone();
                    let rem = rem.clone();

                    tokio::spawn(async move {
                        if 1 == rem.fetch_sub(1, Relaxed) {
                            tx.send(()).unwrap();
                        }
                    });
                }

                rx.recv().unwrap();
            });
        })
    });
}

fn rt_2thr(c: &mut Criterion) {
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    // let rt = rt();

    let (tx, rx) = mpsc::sync_channel(1000);
    let rem = Arc::new(AtomicUsize::new(0));

    const NUM_SPAWN: usize = 1_000_000;

    c.bench_function("rt_2thr", |b| {
        b.iter(|| {
            rem.store(NUM_SPAWN, Relaxed);

            let tx = tx.clone();
            let rem = rem.clone();
            rt.block_on(async {
                tokio::spawn(async move {
                    for _ in 0..NUM_SPAWN {
                        let tx = tx.clone();
                        let rem = rem.clone();

                        tokio::spawn(async move {
                            if 1 == rem.fetch_sub(1, Relaxed) {
                                tx.send(()).unwrap();
                            }
                        });
                    }
                });

                rx.recv().unwrap();
            });
        })
    });
}

fn rt_4thr(c: &mut Criterion) {
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .unwrap();

    // let rt = rt();

    let (tx, rx) = mpsc::sync_channel(1000);
    let rem = Arc::new(AtomicUsize::new(0));

    const NUM_SPAWN: usize = 1_000_000;

    c.bench_function("rt_4thr", |b| {
        b.iter(|| {
            rem.store(NUM_SPAWN, Relaxed);
            let tx = tx.clone();
            let rem = rem.clone();
            let handle = rt.handle().clone();
            rt.block_on(async {
                tokio::spawn(async move {
                    for i in 0..NUM_SPAWN {
                        let tx = tx.clone();
                        let rem = rem.clone();

                        tokio::spawn(async move {
                            if 1 == rem.fetch_sub(1, Relaxed) {
                                tx.send(()).unwrap();
                            }
                        });
                    }
                });

                rx.recv().unwrap();
            });
        })
    });
}

fn rt_block_on_4thr(c: &mut Criterion) {
    const NUM_SPAWN: usize = 1_000_000;
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let (tx, rx) = mpsc::sync_channel(1000);
    let rem = Arc::new(AtomicUsize::new(NUM_SPAWN));

    c.bench_function("rt_block_on_4thr", |b| {
        b.iter_custom(|iterations| {
            let mut total = Duration::ZERO;
            for i in 0..iterations {
                let metrics_data = Arc::new(Mutex::new(Vec::new()));

                let metrics_data_clone = metrics_data.clone();
                let rt_handle = rt.handle().clone();
                let rem_clone = rem.clone();
                let metrics_thread = std::thread::spawn(move || {
                    let time_base = Instant::now();
                    loop {
                        let depth = rt_handle.metrics().global_queue_depth();

                        let now = Instant::now();
                        {
                            let mut data = metrics_data_clone.lock().unwrap();
                            data.push((now, depth));
                        }

                        if rem_clone.load(Relaxed) == 0 {
                            break;
                        }

                        std::thread::sleep(Duration::from_millis(10));
                    }
                });

                // **Measurement Phase (Measured)**
                let start = Instant::now();

                rem.store(NUM_SPAWN, Relaxed);

                rt.block_on(async {
                    for _ in 0..NUM_SPAWN {
                        let tx = tx.clone();
                        let rem = rem.clone();

                        tokio::spawn(async move {
                            if 1 == rem.fetch_sub(1, Relaxed) {
                                tx.send(()).unwrap();
                            }
                        });
                    }

                    rx.recv().unwrap();
                });

                let elapsed = start.elapsed();
                // **End of Measurement Phase**

                metrics_thread.join().unwrap();

                {
                    let data = metrics_data.lock().unwrap();
                    for (_, j) in data.iter() {}
                }

                total += elapsed;
            }
            total
        });
    });
}

fn rt_custom_2thr(c: &mut Criterion) {
    const NUM_SPAWN: usize = 1_000_000;
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (tx, rx) = mpsc::sync_channel(1000);
    let rem = Arc::new(AtomicUsize::new(NUM_SPAWN));

    let file_path = "D:\\git\\tokio\\target\\csv\\metrics_rt_custom_2thr";
    let mut f = File::create(file_path).unwrap();
    f.write("time,total_park_count,total_steal_count,total_steal_operations,num_remote_schedules,total_local_schedule_count,\
    total_overflow_count,total_polls_count,total_busy_duration,injection_queue_depth,total_local_queue_depth\n".as_ref())
        .expect("Error while writing to file");

    c.bench_function("rt_custom_2thr\n", |b| {
        b.iter_custom(|iterations| {
            let tx = tx.clone();
            let rem = rem.clone();
            let mut total = Duration::ZERO;

            for _ in 0..iterations {
                let metrics_data = Arc::new(Mutex::new(Vec::new()));

                let metrics_data_clone = metrics_data.clone();
                let rt_handle = rt.handle().clone();
                let mut met = tokio_metrics::RuntimeMonitor::new(&rt_handle).intervals();
                let rem_clone = rem.clone();
                let metrics_thread = std::thread::spawn(move || {
                    let time_base = Instant::now();
                    loop {
                        let now = Instant::now() - time_base;
                        {
                            let mut data = metrics_data_clone.lock().unwrap();
                            data.push((now, met.next().unwrap()));
                        }

                        if rem_clone.load(Relaxed) == 0 {
                            break;
                        }

                        std::thread::sleep(Duration::from_millis(10));
                    }
                });

                let rem = rem.clone();
                let tx = tx.clone();
                // **Measurement Phase (Measured)**
                let start = Instant::now();

                rem.store(NUM_SPAWN, Relaxed);
                rt.block_on(async {
                    tokio::spawn(async move {
                        for i in 0..NUM_SPAWN {
                            let tx = tx.clone();
                            let rem = rem.clone();

                            tokio::spawn(async move {
                                handle_connection().await;
                                if 1 == rem.fetch_sub(1, Relaxed) {
                                    tx.send(()).unwrap();
                                }
                            });
                            if i % 10_000 == 0 {
                                tokio::task::yield_now().await;
                            }
                        }
                    });

                    rx.recv().unwrap();
                });

                let elapsed = start.elapsed();
                // **End of Measurement Phase**

                metrics_thread.join().unwrap();

                {
                    let data = metrics_data.lock().unwrap();
                    write_metrics(data,&mut f);
                }

                total += elapsed;
            }
            total
        });
    });
}

fn write_metrics(data: MutexGuard<Vec<(Duration, RuntimeMetrics)>>, f: &mut File) {
    let output = data.iter().map(|(time, met)| { format!("{},{},{},{},{},{},{},{},{},{},{}\n",
                                                         time.as_millis(), met.total_park_count, met.total_steal_count,
                                                         met.total_steal_operations, met.num_remote_schedules,
                                                         met.total_local_schedule_count, met.total_overflow_count,
                                                         met.total_polls_count, met.total_busy_duration.as_millis(),
                                                         met.injection_queue_depth, met.total_local_queue_depth) }).collect::<String>();
    f.write_all(output.as_bytes()).expect("Error while writing to file");
}

fn rt_custom_4thr(c: &mut Criterion) {
    const NUM_SPAWN: usize = 1_000_000;
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let (tx, rx) = mpsc::sync_channel(1000);
    let rem = Arc::new(AtomicUsize::new(NUM_SPAWN));
    let file_path = "D:\\git\\tokio\\target\\csv\\metrics_rt_custom_4thr";
    let mut f = File::create(file_path).unwrap();
    f.write("time,total_park_count,total_steal_count,total_steal_operations,num_remote_schedules,total_local_schedule_count,\
    total_overflow_count,total_polls_count,total_busy_duration,injection_queue_depth,total_local_queue_depth\n".as_ref())
        .expect("Error while writing to file");

    c.bench_function("rt_custom_4thr", |b| {
        b.iter_custom(|iterations| {
            let tx = tx.clone();
            let rem = rem.clone();
            let mut total = Duration::ZERO;

            for _ in 0..iterations {
                let metrics_data = Arc::new(Mutex::new(Vec::new()));

                let metrics_data_clone = metrics_data.clone();
                let rt_handle = rt.handle().clone();
                let mut met = tokio_metrics::RuntimeMonitor::new(&rt_handle).intervals();
                let rem_clone = rem.clone();
                let metrics_thread = std::thread::spawn(move || {
                    let time_base = Instant::now();
                    loop {
                        let now = Instant::now() - time_base;
                        {
                            let mut data = metrics_data_clone.lock().unwrap();
                            data.push((now, met.next().unwrap()));
                        }

                        if rem_clone.load(Relaxed) == 0 {
                            break;
                        }

                        std::thread::sleep(Duration::from_millis(10));
                    }
                });
                let rem = rem.clone();
                let tx = tx.clone();

                // **Measurement Phase (Measured)**
                let start = Instant::now();

                rem.store(NUM_SPAWN, Relaxed);
                rt.block_on(async {
                    tokio::spawn(async move {
                        for i in 0..NUM_SPAWN {
                            let tx = tx.clone();
                            let rem = rem.clone();

                            tokio::spawn(async move {
                                handle_connection().await;
                                if 1 == rem.fetch_sub(1, Relaxed) {
                                    tx.send(()).unwrap();
                                }
                            });
                            if i % 10_000 == 0 {
                                tokio::task::yield_now().await;
                            }
                        }
                    });

                    rx.recv().unwrap();
                });

                let elapsed = start.elapsed();
                // **End of Measurement Phase**

                metrics_thread.join().unwrap();

                {
                    let data = metrics_data.lock().unwrap();
                    write_metrics(data, &mut f);
                }

                total += elapsed;
            }
            total
        });
    });
}


async fn handle_connection() {
    let mut sum: u64 = 0;
    // for i in 0..10000 {
    //     unsafe {
    //         let ptr: *mut u64 = &mut sum;
    //         *ptr += i;
    //     }
    // }
    sleep(Duration::from_millis(1)).await;

    tokio::task::yield_now().await;
}

criterion_group!(
    name = rt_queue_overload;
    config = Criterion::default()
        .measurement_time(Duration::new(100, 0))
    .warm_up_time(Duration::new(10, 0)).sample_size(200);
    targets =
    rt_4thr,
    rt_custom_2thr,
    rt_custom_4thr

);

criterion_main!(rt_queue_overload);
