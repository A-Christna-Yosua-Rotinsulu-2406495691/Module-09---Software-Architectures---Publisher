# Tutorial 9 - Publisher

### 1. Seberapa banyak data yang dikirimkan oleh program publisher ke message broker dalam satu kali *run*?

Dalam satu kali eksekusi (satu *run*), program publisher saya mengirimkan **5 buah data (pesan)** ke message broker. 

Hal ini dapat dilihat pada berkas `src/main.rs`, di mana saya memanggil metode `publish_event` sebanyak 5 kali secara berurutan untuk mengirimkan 5 objek `UserCreatedEventMessage` yang berbeda (dengan `user_id` dari "1" sampai "5"). Berikut adalah potongan kode yang menunjukkan pengiriman 5 pesan tersebut:

```rust
fn main() {
    let mut p = CrosstownBus::new_queue_publisher("amqp://guest:guest@localhost:5672".to_owned()).unwrap();
    
    // Mengirim 5 pesan event "user_created" secara berurutan
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

URL `"amqp://guest:guest@localhost:5672"` adalah alamat koneksi (*connection string*) menuju server RabbitMQ (message broker) yang berjalan di lokal komputer saya (`localhost`) pada *port* standar protokol AMQP yaitu `5672`. Bagian `guest:guest` merupakan *username* dan *password* bawaan (*default*) untuk mengakses instance RabbitMQ tersebut.

Kesamaan URL antara program **publisher** dan **subscriber** ini berarti **kedua program tersebut terhubung ke _instance_ message broker (RabbitMQ) yang persis sama**. Hal ini sangat penting agar proses pertukaran pesan dapat terjadi. Karena mereka berdua menunjuk dan terhubung ke tempat yang sama, maka pesan-pesan yang dikirim (dipublikasikan) oleh program publisher ke message broker nantinya dapat dibaca dan diterima dengan baik oleh program subscriber yang "mendengarkan" (*listen* / *subscribe*) pada antrean (*queue*) di message broker tersebut.

Berikut potongan kode inisialisasi pada `src/main.rs` publisher yang menunjukkan penggunaan URL koneksi tersebut:
```rust
    let mut p = CrosstownBus::new_queue_publisher("amqp://guest:guest@localhost:5672".to_owned()).unwrap();
```
