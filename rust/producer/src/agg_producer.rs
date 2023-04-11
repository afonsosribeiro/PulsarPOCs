use std::thread;
use std::time;
use rand::Rng;

use log::info;
use pulsar::TokioExecutor;



fn chm_clog_msg(i_trckng_id: usize, nm: &str, drtn:usize, s_cd: &str, oprtn: usize) -> String {
    format!(r#"{{"dmn": "Channel Manager", "stp": 1, "systm": "WAS", "iTrckngID": {}, "nm": "{}", "drtn": {}, "strtTm": "{}", "sCd": "{}", "oprtn": "A{}", "isErr": false, "bsnssKyLst": [[{{"fld": "PROCESSING_MODE", "vl": "ARCTIC" }}], [{{ "fld": "ACCEPTANCE_NETWORK_COD", "vl": "MULTIB" }}], [{{ "fld": "ACCEPTANCE_CHANNEL_TYPE_COD", "vl": "ATM" }}]]}}"#,
    i_trckng_id, nm, drtn, chrono::offset::Local::now(), s_cd, oprtn)
}

fn acq_clog_msg(i_trckng_id: usize, drtn:usize) -> String {
    format!(r#"{{"dmn": "Acquiring", "stp": 2, "iTrckngID": {}, "drtn": {}, "strtTm": "{}"}}"#,
    i_trckng_id, drtn, chrono::offset::Local::now())
}

fn iss_clog_msg(i_trckng_id: usize, drtn:usize, bank_code:usize) -> String {
    format!(r#"{{"dmn": "Issuing", "stp": 3, "iTrckngID": {}, "drtn": {}, "strtTm": "{}", "systm": "Postilion", "bsnssKyLst": [[{{"fld": "BANK_CDE", "vl": "{}" }}],[{{ "fld": "AUTH_SCENARIO", "vl": "10 - Realtime with issuer"}}]]}}"#,
    i_trckng_id, drtn, chrono::offset::Local::now(), bank_code)
}

fn ecm_clog_msg(i_trckng_id: usize, drtn:usize) -> String {
    format!(r#"{{"dmn": "SWI ECM", "stp": 4, "iTrckngID": {}, "drtn": {}, "strtTm": "{}"}}"#,
    i_trckng_id, drtn, chrono::offset::Local::now())
}

fn prt_clog_msg(i_trckng_id: usize, drtn:usize) -> String {
    format!(r#"{{"dmn": "SWI PRT", "stp": 5, "iTrckngID": {}, "drtn": {}, "strtTm": "{}"}}"#,
    i_trckng_id, drtn, chrono::offset::Local::now())
}

fn agg_msg(i_trckng_id: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut agg_msg = vec![];

    agg_msg.extend("[".as_bytes());

    for msg in [
        chm_clog_msg(i_trckng_id, "ORI-", rng.gen_range(10..500), "COM-000", rng.gen_range(100..999)),
        acq_clog_msg(i_trckng_id, rng.gen_range(10..400)),
        iss_clog_msg(i_trckng_id, rng.gen_range(10..300), rng.gen_range(1000..9999)),
        ecm_clog_msg(i_trckng_id, rng.gen_range(10..200)),
        prt_clog_msg(i_trckng_id, rng.gen_range(10..100))
    ] {
        agg_msg.extend(msg.as_bytes());
        agg_msg.extend(",".as_bytes());
    }

    if agg_msg.len() > 1 {
        agg_msg.pop();
    }

    agg_msg.extend("]".as_bytes());

    //info!("{:}", std::str::from_utf8(&agg_msg).unwrap());
    agg_msg

}

pub async fn agg_producer(producer: &mut pulsar::Producer<TokioExecutor>) -> Result<(), pulsar::Error> {
    let mut counter = 0usize;
    let mut i = 0usize;
    loop {
        i += 1;
        producer.create_message()
            .with_key(format!("{i}"))
            .with_content(agg_msg(i))
            .send()
            .await?;
        counter += 1;

        if counter%100_000 == 0 {
            log::info!("sent {counter} messages");
        }

        // if counter%200 == 0 {
        //     log::info!("sent {counter} messages");
        // }


        //thread::sleep(time::Duration::from_millis(10));
    }
}