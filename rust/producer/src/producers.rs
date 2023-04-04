use std::thread;
use std::time;

use log::info;
use pulsar::TokioExecutor;

pub async fn simple_producer(producer: &mut pulsar::Producer<TokioExecutor>) -> Result<(), pulsar::Error> {
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

// (
//     '[' +
//         '{' +
//             '"iTrckngID": "%d", ' +
//             '"nm": "Channel Manager", ' +
//             '"drtn": "%d", ' +
//             '"strtTm": "%s"' +
//         '}, {' +
//             '"iTrckngID": "%d", ' +
//             '"nm": "Issuing", ' +
//             '"drtn": "%d", ' +
//             '"strtTm": "%s"' +
//         '}' +
//     ']'
// )

fn clog_msg(i: usize, j:usize) -> String {
    format!("{{\"iTrckngID\": {}, \"nm\": \"{}\", \"drtn\": {}, \"strtTm\": {}}}", i, "Channel Mannager", i+j, chrono::offset::Local::now())
}

fn agg_msg(i: usize, j:usize) -> Vec<u8> {
    let mut agg_msg = vec![];

    agg_msg.extend("[".as_bytes());

    for msg in [clog_msg(i, j), clog_msg(i, j)] {
        agg_msg.extend(msg.as_bytes());
        agg_msg.extend(",".as_bytes());
    }

    if agg_msg.len() > 1 {
        agg_msg.pop();
    }

    agg_msg.extend("]".as_bytes());

    info!("{:}", std::str::from_utf8(&agg_msg).unwrap());
    agg_msg

}

pub async fn agg_producer(producer: &mut pulsar::Producer<TokioExecutor>) -> Result<(), pulsar::Error> {
    let mut counter = 0usize;
    let mut i = 0usize;
    loop {
        i += 1;
        for j in 1..11usize {
            producer.create_message()
                .with_key(format!("{i}"))
                .with_content(agg_msg(i, j))
                .send()
                .await?;
            counter += 1;
        }

        if counter%100_000 == 0 {
            log::info!("sent {counter} messages");
        }

        // if counter%200 == 0 {
        //     log::info!("sent {counter} messages");
        // }


        // thread::sleep(time::Duration::from_millis(100));
    }
}