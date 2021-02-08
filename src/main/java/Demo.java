import com.kevi793.EventStorageAndProcessing.store.EventStore;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

public class Demo {

    private static final int WORK_PER_THREAD = 50;
    private static final String DIR = "/Users/nexus/Desktop/Java/FileStorageAndProcessing/data";

    public static void main(String[] args) throws InterruptedException, IOException {

        Consumer<Item> consumer = (c) -> System.out.println("Got from consumer: " + c);

        EventStore eventStore = new EventStore.EventStoreBuilder<Item>(DIR, "tracking-service", consumer, Item.class)
                .fileSegmentCacheSize(1024)
                .maxSegmentLogFileSizeInBytes(1024L)
                .build();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            threads.add(new GenerateLogItem(eventStore));
            threads.get(i).start();
        }

        for (int i = 0; i < 4; i++) {
            threads.get(i).join();
        }

        Thread.sleep(100000);
    }

    private static class GenerateLogItem extends Thread {

        private final EventStore eventStore;

        public GenerateLogItem(EventStore eventStore) {
            this.eventStore = eventStore;
        }

        @SneakyThrows
        @Override
        public void run() {
            for (int i = 0; i < 20; ) {
                eventStore.write(new Item(Thread.currentThread().getName(), new Random().nextInt()));
                Thread.sleep(100);
            }
        }

    }

    @Getter
    @NoArgsConstructor
    private static class Item {
        private String name;
        private int price;

        public Item(String name, int price) {
            this.name = name;
            this.price = price;
        }
    }

    private static class ArrayThread extends Thread {

        private final Random random;
        private final List<Integer> list;

        public ArrayThread(List<Integer> list) {
            this.list = list;
            this.random = new Random();
        }

        @Override
        public void run() {
            for (int i = 0; i < WORK_PER_THREAD; i++) {
                this.list.add(random.nextInt());
            }
        }
    }

}
