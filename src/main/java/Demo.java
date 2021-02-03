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

        LogFile logFile = new LogFile(DIR, "tracking-service", 1024L, 1024);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            threads.add(new GenerateLogItem(logFile));
            threads.get(i).start();
        }

        for (int i = 0; i < 4; i++) {
            threads.get(i).join();
        }

        System.out.println("Offset 2: \n" + logFile.read(2));
        System.out.println("Offset 10: \n" + logFile.read(10));
        System.out.println("Offset 3: \n" + logFile.read(3));
        System.out.println("Offset 15: \n" + logFile.read(15));
        System.out.println("Offset 11: \n" + logFile.read(11));
        System.out.println("Offset 199: \n" + logFile.read(199));

    }

    private static void multithreadListInsertion() throws InterruptedException {
        List<Integer> list = new ArrayList<Integer>();
        List<Thread> threads = new ArrayList<Thread>();

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.add(new ArrayThread(list));
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).start();
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            threads.get(i).join();
        }

        System.out.println("Actual List length is: " + list.size());
        System.out.println("Expected List length is: " + NUM_THREADS * WORK_PER_THREAD);
    }

    private static class GenerateLogItem extends Thread {

        private final LogFile logFile;

        public GenerateLogItem(LogFile logFile) {
            this.logFile = logFile;
        }

        @SneakyThrows
        @Override
        public void run() {
            for (int i = 0; i < 20; i++) {
                logFile.write(new Item(Thread.currentThread().getName(), new Random().nextInt()));
            }
        }

    }

    @Getter
    private static class Item {
        private final String name;
        private final int price;

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
