#![no_std]
#![no_main]
#![deny(
    clippy::mem_forget,
    reason = "mem::forget is generally not safe to do with esp_hal types, especially those \
    holding buffers for the duration of a data transfer."
)]
#![deny(clippy::large_stack_frames)]

use embassy_executor::Spawner;
use embassy_net::Runner;
use embassy_time::{Duration, Timer};
use esp_backtrace as _;
use esp_hal::clock::CpuClock;
use esp_hal::timer::timg::TimerGroup;
use esp_radio::wifi::{
    ClientConfig, ModeConfig, ScanConfig, WifiController, WifiDevice, WifiEvent, WifiStaState,
};
use log::{error, info};

extern crate alloc;

// This creates a default app-descriptor required by the esp-idf bootloader.
// For more information see: <https://docs.espressif.com/projects/esp-idf/en/stable/esp32/api-reference/system/app_image_format.html#application-description>
esp_bootloader_esp_idf::esp_app_desc!();

macro_rules! mk_static {
    ($t:ty,$val:expr) => {{
        static STATIC_CELL: static_cell::StaticCell<$t> = static_cell::StaticCell::new();
        #[deny(unused_attributes)]
        let x = STATIC_CELL.uninit().write(($val));
        x
    }};
}

const SSID: &str = env!("WIFI_SSID");
const PASSWORD: &str = env!("WIFI_PASSWORD");

#[allow(
    clippy::large_stack_frames,
    reason = "it's not unusual to allocate larger buffers etc. in main"
)]
#[esp_rtos::main]
async fn main(spawner: Spawner) -> ! {
    // generator version: 1.2.0

    esp_println::logger::init_logger_from_env();

    let config = esp_hal::Config::default().with_cpu_clock(CpuClock::max());
    let mut peripherals = esp_hal::init(config);

    esp_alloc::heap_allocator!(#[esp_hal::ram(reclaimed)] size: 98768);

    let timg0 = TimerGroup::new(peripherals.TIMG0);
    esp_rtos::start(timg0.timer0);

    info!("Embassy initialized!");

    // Initialize the TRNG source
    let _trng_source = esp_hal::rng::TrngSource::new(peripherals.RNG, peripherals.ADC1.reborrow());

    let mut trng = esp_hal::rng::Trng::try_new().expect("Failed to initialize TRNG");

    let esp_radio_ctrl = &*mk_static!(
        esp_radio::Controller<'static>,
        esp_radio::init().expect("Failed to initialize radio controller")
    );

    let (controller, interfaces) =
        esp_radio::wifi::new(esp_radio_ctrl, peripherals.WIFI, Default::default())
            .expect("Failed to initialize wifi");

    let wifi_interface = interfaces.sta;

    let config = embassy_net::Config::dhcpv4(Default::default());

    let seed = (trng.random() as u64) << 32 | trng.random() as u64;

    // Init network stack
    let (stack, runner) = embassy_net::new(
        wifi_interface,
        config,
        mk_static!(
            embassy_net::StackResources<3>,
            embassy_net::StackResources::<3>::new()
        ),
        seed,
    );

    spawner
        .spawn(connection(controller))
        .expect("Failed to spawn connection task");
    spawner
        .spawn(net_task(runner))
        .expect("Failed to spawn net task");

    loop {
        if stack.is_link_up() {
            break;
        }

        Timer::after(Duration::from_millis(500)).await;
    }

    info!("Waiting to get IP address...");
    loop {
        if let Some(config) = stack.config_v4() {
            info!("Got IP: {}", config.address);
            break;
        }

        Timer::after(Duration::from_millis(500)).await;
    }

    let mut rx_buffer = [0; 1024];
    let mut tx_buffer = [0; 1024];

    let domain = "websockets.chilkat.io";
    let ip = *stack
        .dns_query(domain, smoltcp::wire::DnsQueryType::A)
        .await
        .expect("DNS query failed")
        .first()
        .expect("No IP address returned");

    info!("Resolved {domain} to {ip}");

    loop {
        Timer::after(Duration::from_millis(1_000)).await;

        let mut socket = embassy_net::tcp::TcpSocket::new(stack, &mut rx_buffer, &mut tx_buffer);

        socket.set_timeout(Some(embassy_time::Duration::from_secs(2)));

        info!("Connecting...");

        let r = socket.connect((ip, 80)).await;

        if let Err(e) = r {
            error!("Connect error: {:?}", e);

            continue;
        }

        info!("Connected!");

        let mut read_buf = [0u8; 1024];
        let mut write_buf = [0u8; 1024];
        let mut fragments_buf = [0u8; 1024];

        let rng = RngCompat { inner: &mut trng };
        let mut websocketz = websocketz::WebSocket::connect::<16>(
            websocketz::options::ConnectOptions::default()
                .with_path_unchecked("/wsChilkatEcho.ashx")
                .with_headers(&[websocketz::http::Header {
                    name: "Host",
                    value: domain.as_bytes(),
                }]),
            &mut socket,
            rng,
            &mut read_buf,
            &mut write_buf,
            &mut fragments_buf,
        )
        .await
        .expect("Failed to create WebSocket connection");

        // split the WebSocket into read and write halves
        // let (mut websocketz_read, mut websocketz_write) = websocketz.split_with(|socket| socket.split());

        'ws: loop {
            websocketz
                .send(websocketz::Message::Text("Hello, WebSocket!"))
                .await
                .expect("Failed to send message");

            match websocketz::next!(websocketz) {
                None => {
                    info!("EOF");

                    break 'ws;
                }
                Some(Ok(msg)) => {
                    info!("Received message: {:?}", msg);
                }
                Some(Err(e)) => {
                    error!("Error receiving message: {:?}", e);

                    break 'ws;
                }
            }

            Timer::after(Duration::from_millis(1000)).await;
        }

        info!("Closing connection...");
    }
}

#[allow(
    clippy::large_stack_frames,
    reason = "it's not unusual to allocate larger buffers etc. in wifi connection tasks"
)]
#[embassy_executor::task]
async fn connection(mut controller: WifiController<'static>) {
    info!("start connection task");
    info!("Device capabilities: {:?}", controller.capabilities());
    loop {
        if esp_radio::wifi::sta_state() == WifiStaState::Connected {
            // wait until we're no longer connected
            controller.wait_for_event(WifiEvent::StaDisconnected).await;
            Timer::after(Duration::from_millis(5000)).await
        }
        if !matches!(controller.is_started(), Ok(true)) {
            let client_config = ModeConfig::Client(
                ClientConfig::default()
                    .with_ssid(SSID.into())
                    .with_password(PASSWORD.into()),
            );
            controller.set_config(&client_config).unwrap();
            info!("Starting wifi");
            controller.start_async().await.unwrap();
            info!("Wifi started!");

            info!("Scan");
            let scan_config = ScanConfig::default().with_max(10);
            let result = controller
                .scan_with_config_async(scan_config)
                .await
                .unwrap();
            for ap in result {
                info!("Found AP: {:?}", ap);
            }
        }
        info!("About to connect...");

        match controller.connect_async().await {
            Ok(_) => info!("Wifi connected!"),
            Err(e) => {
                info!("Failed to connect to wifi: {e:?}");
                Timer::after(Duration::from_millis(5000)).await
            }
        }
    }
}

#[embassy_executor::task]
async fn net_task(mut runner: Runner<'static, WifiDevice<'static>>) {
    runner.run().await
}

/// Compat for rand_core 0.9 and 0.10
///
/// Websocketz uses rand_core 0.10, but esp-hal's TRNG implements the 0.9 traits.
struct RngCompat<T> {
    inner: T,
}

impl<T> rand_core_10::TryRng for RngCompat<T>
where
    T: rand_core_09::RngCore,
{
    type Error = core::convert::Infallible;

    fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
        Ok(self.inner.next_u32())
    }

    fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
        Ok(self.inner.next_u64())
    }

    fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Self::Error> {
        self.inner.fill_bytes(dst);
        Ok(())
    }
}
