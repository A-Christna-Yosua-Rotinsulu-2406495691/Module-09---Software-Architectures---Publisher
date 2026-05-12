# Tutorial 9 - Publisher

***By Christna Yosua Rotinsulu - 2406495691***

### 1. Seberapa banyak data yang dikirimkan oleh program publisher ke message broker dalam satu kali *run*?

Dalam satu kali eksekusi program (satu *run*), program *publisher* yang saya jalankan mengirimkan **5 buah data (pesan)** ke *message broker*. 

Hal ini bisa dilihat pada berkas `src/main.rs`, di mana saya secara eksplisit memanggil metode `publish_event` sebanyak 5 kali secara berurutan. Pesan yang saya kirimkan berupa *event* bertipe `UserCreatedEventMessage` dengan identitas `user_id` yang berbeda-beda, mulai dari "1" hingga "5". 

Untuk menambah keterjelasan, berikut adalah keseluruhan isi kode dari `src/main.rs` yang saya buat untuk memfasilitasi pembuatan hingga pengiriman pesan tersebut:

```rust
use borsh::{BorshDeserialize, BorshSerialize};
use crosstown_bus::{CrosstownBus, MessageHandler, HandleError};

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize)]
pub struct UserCreatedEventMessage {
    pub user_id: String,
    pub user_name: String
}

pub struct UserCreatedHandler;

impl MessageHandler<UserCreatedEventMessage> for UserCreatedHandler {
    fn handle(&self, message: Box<UserCreatedEventMessage>) -> Result<(), HandleError> {
        println!("Message received on handler 1: {:?}", message);
        Ok(())
    }

    fn get_handler_action(&self) -> String {
        "user_created".to_string()
    }
}

fn main() {
    let mut p = CrosstownBus::new_queue_publisher("amqp://guest:guest@54.208.122.143:5672".to_owned()).unwrap();
    
    // Saya mengirim 5 pesan event "user_created" ke dalam message broker secara berurutan
    _ = p.publish_event("user_created".to_owned(), UserCreatedEventMessage {
        user_id: "1".to_owned(), user_name: "2406495691-Amir".to_owned() });
    _ = p.publish_event("user_created".to_owned(), UserCreatedEventMessage {
        user_id: "2".to_owned(), user_name: "2406495691-Budi".to_owned() });
    _ = p.publish_event("user_created".to_owned(), UserCreatedEventMessage {
        user_id: "3".to_owned(), user_name: "2406495691-Cica".to_owned() });
    _ = p.publish_event("user_created".to_owned(), UserCreatedEventMessage {
        user_id: "4".to_owned(), user_name: "2406495691-Dira".to_owned() });
    _ = p.publish_event("user_created".to_owned(), UserCreatedEventMessage {
        user_id: "5".to_owned(), user_name: "2406495691-Emir".to_owned() });
}
```

### 2. URL `"amqp://guest:guest@localhost:5672"` sama dengan yang ada di program subscriber, apa artinya?

URL `"amqp://guest:guest@localhost:5672"` adalah alamat koneksi (*connection string*) menuju *server* RabbitMQ (*message broker*) yang berjalan di lokal komputer saya (`localhost`) pada *port* standar protokol AMQP yaitu `5672`. Bagian `guest:guest` merupakan *username* dan sandi bawaan (*default*) untuk mengakses *instance* RabbitMQ tersebut.

Kesamaan URL antara program **publisher** dan **subscriber** ini berarti **kedua program saya terhubung ke *instance message broker* (RabbitMQ) yang persis sama**. Hal ini sangat krusial agar proses pertukaran pesan antarlayanan dapat terjadi. Karena kedua program menunjuk pada antrean yang sama, pesan yang saya publikasikan oleh program *publisher* ke *message broker* pasti dapat disalurkan dan dibaca dengan tepat oleh *subscriber* yang sedang mendengarkan (*subscribe*).

Berikut potongan kode inisialisasi pada `src/main.rs` publisher yang menunjukkan penggunaan URL koneksi tersebut:
```rust
    let mut p = CrosstownBus::new_queue_publisher("amqp://guest:guest@54.208.122.143:5672".to_owned()).unwrap();
```

### RabbitMQ Interface
![RabbitMQ Interface](assets/images/RabbitMQ-Interface.png)

### RabbitMQ Connection
![RabbitMQ Connection](assets/images/RabbitMQ-Connection.png)
![5 Message Events](assets/images/5MessageEvent.png)

Setelah saya menjalankan baik program *publisher* maupun *subscriber*, saya bisa langsung melihat adanya koneksi yang aktif pada antarmuka manajemen RabbitMQ. Hal ini memverifikasi bahwa program-program tersebut telah berhasil terhubung ke *message broker* tanpa kendala dan siap untuk melakukan pertukaran data.

Saat saya mengeksekusi program *publisher* melalui perintah `cargo run`, program tersebut dengan instan akan mengirimkan 5 pesan *event* berturut-turut ke *message broker*. Karena terhubung dalam alamat selaras, rentetan pesan ini otomatis dikonsumsi dan diproses oleh *subscriber* yang telah berjaga (*listening*) pada sisi antrean yang sama.

### RabbitMQ Monitoring
![RabbitMQ Monitoring](assets/images/RabbitMQ-SubscriberConnection.png)
![5 Message Events Spike](assets/images/5MessageEventSpike.png)

Grafik *monitoring* pada RabbitMQ menunjukkan kemunculan garis lonjakan tajam (*spike*) seketika program *publisher* dieksekusi. Lonjakan ini merepresentasikan aktivitas masuknya beban pengiriman pesan dari *publisher* ke *message broker* dalam waktu yang amat singkat (5 pesan serentak). Setelah lonjakan itu tertampung di dalam antrean, grafik beralih menunjukkan aktivitas pemrosesan secara bertahap oleh *subscriber* yang aktif, sehingga jelas membuktikan berhasilnya mekanisme aliran pesan asinkronus antara dua program saya.

### Simulation Slow Subscriber
![Console Connection Sleep](assets/images/ConsoleConnectionSleep.png)
![RabbitMQ Connection Sleep](assets/images/RabbitMQ-ConnectionSleep.png)

Pada gambar percobaan simulasi di atas, dapat dicermati bahwa *producer* (program *publisher* saya) dapat terus-menerus menembakkan data tanpa henti ke dalam tumpukan antrean pesan. Selanjutnya secara perlahan dari batas memori RabbitMQ, program *consumer* (*subscriber*) saya menarik dan mengolah pekerjaan tersebut satu per satu.

## Simulation Spike Reduce

![Console Spike Reduce](assets/images/ConsoleSpikeReduce.png)
![RabbitMQ Spike Reduce](assets/images/RabbitMQ-SpikeReduce.png)

### Refleksi/Jawaban
Pola ini menunjukkan bagaimana *message broker* (RabbitMQ) berfungsi sebagai **buffer** untuk menangani **spike** (lonjakan pesan tiba-tiba). Meskipun publisher mengirimkan banyak pesan dalam waktu yang sangat singkat (seperti terlihat pada gambar konsol di mana publisher selesai jauh sebelum subscriber), RabbitMQ menampung semua pesan tersebut di dalam antrean. 

Subscriber kemudian memproses pesan-pesan tersebut secara stabil satu per satu (karena ada *delay* buatan). Hal ini mencegah subscriber menjadi *overloaded* atau *crash* akibat lonjakan beban mendadak. Inilah yang disebut sebagai strategi **Load Leveling** atau **Spike Arresting**.

### Apa yang bisa ditingkatkan? (Improvements)

Berdasarkan kode yang ada sekarang, beberapa hal yang dapat ditingkatkan antara lain:

1. **Error Handling pada Publisher & Subscriber**:
   - Di `publisher`, pemanggilan `publish_event` menggunakan `_ =` yang berarti mengabaikan hasil (Result). Sebaiknya ada pengecekan apakah pesan berhasil terkirim ke broker atau tidak.
   - Di `subscriber`, jika proses pengolahan pesan gagal, kita bisa menambahkan logika *retry* atau memindahkan pesan ke *Dead Letter Exchange* (DLX) untuk dianalisis lebih lanjut.

2. **Concurrency pada Subscriber**:
   - Saat ini subscriber memproses pesan satu per satu secara sekuensial. Jika volume pesan sangat besar, kita bisa meningkatkan performa dengan menjalankan beberapa *worker threads* untuk memproses pesan secara paralel, namun tetap dengan batas tertentu (*rate limiting*) agar tidak membebani sistem.

3. **Menghindari Busy Loop**:
   - Di `main.rs` subscriber, terdapat `loop {}` yang kosong. Ini adalah *busy-wait* yang mengonsumsi CPU secara sia-sia. Sebaiknya gunakan mekanisme sinkronisasi yang lebih baik atau biarkan main thread menunggu sinyal berhenti secara elegan.

4. **Konfigurasi Eksternal**:
    - URL koneksi `"amqp://guest:guest@54.208.122.143:5672"` saat ini di-*hardcode*. Sebaiknya dipindahkan ke variabel lingkungan (*environment variables*) atau file konfigurasi agar lebih fleksibel saat *deployment*.

## Bonus: Running on Cloud (AWS EC2)

### Make it Works

Setelah berhasil menjalankan RabbitMQ di lokal, saya mencoba memindahkannya ke *cloud* menggunakan AWS EC2. Saya mengonfigurasi *Security Group* pada EC2 untuk membuka port 5672 (AMQP) dan 15672 (Management UI) agar dapat diakses secara eksternal.

Berikut adalah tampilan konsol saat saya menjalankan program *subscriber* dan *publisher* yang terhubung ke IP publik EC2 saya:

![Console Make It Work EC2](assets/images/ConsoleMakeItWork-EC2.png)

Saya juga memverifikasi koneksi tersebut melalui *dashboard* RabbitMQ Management yang berjalan di EC2. Terlihat bahwa terdapat koneksi aktif yang masuk dari komputer saya menuju *server* di AWS:

![RabbitMQ Make It Work EC2](assets/images/RabbitMQ-MakeItWork-EC2.png)

Hal ini membuktikan bahwa arsitektur pengiriman pesan asinkronus ini dapat berjalan dengan baik di lingkungan *cloud* asalkan konfigurasi jaringan dan *firewall* sudah tepat.

### Monitoring Spike di EC2

Selain memverifikasi koneksi, saya juga memantau grafik aktivitas pada *dashboard* RabbitMQ saat program *publisher* dijalankan. Terjadi sebuah lonjakan tajam (*spike*) pada grafik *Message Rate* sesaat setelah saya mengeksekusi perintah `cargo run` pada *publisher* di terminal lokal saya.

![RabbitMQ Spike EC2](assets/images/RabbitMQ-Spike-EC2.png)

Lonjakan ini memiliki kaitan erat dengan bagaimana program *publisher* saya bekerja. Di dalam `main.rs`, saya memerintahkan program untuk mengirimkan 5 buah pesan *event* secara berturut-turut tanpa jeda waktu. Berikut adalah potongan kode pemicunya:

```rust
// publisher/src/main.rs

    _ = p.publish_event("user_created".to_owned(), UserCreatedEventMessage {
        user_id: "1".to_owned(), user_name: "2406495691-Amir".to_owned() });
    _ = p.publish_event("user_created".to_owned(), UserCreatedEventMessage {
        user_id: "2".to_owned(), user_name: "2406495691-Budi".to_owned() });
    _ = p.publish_event("user_created".to_owned(), UserCreatedEventMessage {
        user_id: "3".to_owned(), user_name: "2406495691-Cica".to_owned() });
    _ = p.publish_event("user_created".to_owned(), UserCreatedEventMessage {
        user_id: "4".to_owned(), user_name: "2406495691-Dira".to_owned() });
    _ = p.publish_event("user_created".to_owned(), UserCreatedEventMessage {
        user_id: "5".to_owned(), user_name: "2406495691-Emir".to_owned() });
```

Karena pesan-pesan tersebut dikirimkan hampir secara instan ke RabbitMQ yang ada di EC2, sistem *monitoring* mencatat adanya beban kerja yang masuk secara tiba-tiba dalam satu waktu. Jika *subscriber* sedang sibuk atau memiliki waktu pemrosesan (seperti yang saya simulasikan dengan `thread::sleep` di bawah), maka pesan-pesan ini akan menumpuk sebentar di antrean, menciptakan *spike* yang terlihat jelas pada grafik.

```rust
// subscriber/src/main.rs

    fn handle(&self, message: Box<UserCreatedEventMessage>) -> Result<(), HandleError> {
        let ten_millis = time::Duration::from_millis(1000); // Simulasi pemrosesan lambat
        thread::sleep(ten_millis);

        println!("In Christna Yosua Rotinsulu's Computer [2406495691]. Message received: {:?}", message);
        Ok(())
    }
```

### Simulation Slow Subscriber di EC2

Eksperimen terakhir yang saya lakukan di cloud adalah mensimulasikan *slow subscriber*. Saya sengaja membuat *subscriber* memproses pesan lebih lambat daripada kecepatan *publisher* mengirimkan data untuk melihat bagaimana RabbitMQ menangani penumpukan beban.

![RabbitMQ Slow Connection EC2](assets/images/RabbitMQ_SlowConnection-EC2.png)

**Mengapa jumlah pesan dalam antrean bisa mencapai angka tertentu (dalam percobaan saya mencapai 20)?**

Angka 20 pada antrean tersebut merupakan hasil akumulasi dari pesan-pesan yang sudah dikirim oleh *publisher* namun belum sempat diproses oleh *subscriber*. Penjelasannya adalah sebagai berikut:

1. **Sisi Publisher**: Setiap kali saya menjalankan perintah `cargo run` pada project *publisher*, program tersebut akan mengirimkan **5 buah pesan** sekaligus ke RabbitMQ.
   ```rust
   // publisher/src/main.rs
   _ = p.publish_event("user_created".to_owned(), UserCreatedEventMessage { ... }); // Dipanggil 5 kali
   ```

2. **Sisi Subscriber**: Saya memodifikasi *subscriber* agar bekerja sangat lambat dengan menambahkan `thread::sleep` selama 1 detik (1000 ms) untuk setiap pesan yang diterima.
   ```rust
   // subscriber/src/main.rs
   let ten_millis = time::Duration::from_millis(1000);
   thread::sleep(ten_millis);
   ```

3. **Akumulasi**: Ketika saya menjalankan program *publisher* sebanyak **4 kali berturut-turut** (4 kali run x 5 pesan = 20 pesan), pesan-pesan tersebut masuk ke RabbitMQ dalam waktu yang sangat singkat. Karena *subscriber* membutuhkan waktu total 20 detik untuk menyelesaikan semuanya, maka pesan-pesan yang mengantre tersebut akan terlihat menumpuk di dashboard RabbitMQ hingga mencapai angka 20.

Hal ini membuktikan bahwa RabbitMQ di AWS EC2 berfungsi dengan sangat baik sebagai *buffer* penengah, memastikan tidak ada pesan yang hilang meskipun *subscriber* saya sedang berjalan sangat lambat.

### Referensi
RabbitMQ. (n.d.-a). *Dead letter exchanges*. Retrieved May 12, 2026, from https://www.rabbitmq.com/docs/dlx

RabbitMQ. (n.d.-b). *RabbitMQ tutorial - Publish/Subscribe*. Retrieved May 12, 2026, from https://www.rabbitmq.com/tutorials/tutorial-three-python
