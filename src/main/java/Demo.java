import lombok.Getter;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Demo {

    private static final int NUM_THREADS = 10;
    private static final int WORK_PER_THREAD = 50;
    private static final String DIR = "/Users/nexus/Desktop/Java/FileStorageAndProcessing/data";

    public static void main(String[] args) throws InterruptedException, IOException {
        LogFileSegment<Item> fileSegment = new LogFileSegment<Item>(DIR, "tracking-service", 5);
        List<Thread> threads = new ArrayList<>();
        for (int i=0;i<4;i++){
            threads.add(new GenerateLogItem(fileSegment));
            threads.get(i).start();
        }

        for (int i=0;i<4;i++) {
            threads.get(i).join();
        }

        System.out.println("Offset 2: \n" + fileSegment.read(2));
        System.out.println("Offset 10: \n" + fileSegment.read(10));
        System.out.println("Offset 3: \n" + fileSegment.read(3));
        System.out.println("Offset 15: \n" + fileSegment.read(15));
        System.out.println("Offset 11: \n" + fileSegment.read(11));
        System.out.println("Offset 199: \n" + fileSegment.read(199));

    }

    private static class GenerateLogItem extends Thread {

        private final LogFileSegment<Item> logFileSegment;

        public GenerateLogItem(LogFileSegment<Item> logFileSegment) {
            this.logFileSegment = logFileSegment;
        }

        @SneakyThrows
        @Override
        public void run() {
            for (int i=0;i<50;i++) {
                logFileSegment.append(new Item(Thread.currentThread().getName(), new Random().nextInt()));
            }
        }

    }

    @Getter
    private static class Item {
        private String name;
        private int price;

        public Item(String name, int price) {
            this.name = name;
            this.price = price;
        }
    }

    private static void multithreadListInsertion() throws InterruptedException {
        List<Integer> list = new ArrayList<Integer>();
        List<Thread> threads = new ArrayList<Thread>();

        for (int i=0;i<NUM_THREADS;i++) {
            threads.add(new ArrayThread(list));
        }

        for (int i=0;i<NUM_THREADS;i++) {
            threads.get(i).start();
        }

        for (int i=0;i<NUM_THREADS;i++) {
            threads.get(i).join();
        }

        System.out.println("Actual List length is: " + list.size());
        System.out.println("Expected List length is: " + NUM_THREADS*WORK_PER_THREAD);
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
            for (int i=0;i<WORK_PER_THREAD;i++) {
                this.list.add(random.nextInt());
            }
        }
    }

}
