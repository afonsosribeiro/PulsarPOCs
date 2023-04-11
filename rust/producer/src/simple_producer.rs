use std::thread;
use std::time;
use rand::Rng;

use log::info;
use pulsar::TokioExecutor;

pub async fn _simple_producer(producer: &mut pulsar::Producer<TokioExecutor>) -> Result<(), pulsar::Error> {
    let mut counter = 0usize;
    let mut i = 0usize;
    loop {
        i += 1;
        for j in 1..11 {
            producer.create_message()
                .with_key(format!("{i}"))
                .with_content(format!("{{id: {i}, mssg: \"Go dance mamans {}\" {}}}", i+j, (0..200).map(|_| "X").collect::<String>()))
                .send()
                .await?;
            counter += 1;
        }

        if counter%100_000 == 0 {
            log::info!("sent {counter} messages");
        }

        // if counter%10_000 == 0 {
        //     log::info!("sent {counter} messages");
        // }


        //thread::sleep(time::Duration::from_millis(100));
    }
}
