# FileStorageAndProcessing
A library that helps with:
* Accepting events and storing it reliably (flushing into file on disk)
* Process the events (some business logic to perform on the event, given by the client)
* Purge the processed events.

## Usage: 

Suppose the incoming event is of type Price, defined as

```java
  @Getter
  @NoArgsConstructor
  class Item {
      private String name;
      private int price;

      public Item(String name, int price) {
          this.name = name;
          this.price = price;
      }
  }
```

To create an event store, and set up processing and purging,

```java
  private static final String DIR = "/tmp/FileStorageAndProcessing/data";
  private static final String ENTITY_IDENTIFIER = "purchased-items";

  Consumer<Item> consumer = (item) -> System.out.println("Display the item: " + item.toString());

  EventStore<Item> eventStore = new EventStore.EventStoreBuilder<Item>(baseDirectoryPath, "entityIdentifier", consumer, Item.class)
                  .fileSegmentCacheSize(1024)
                  .maxSegmentLogFileSizeInBytes(10240L)
                  .eventProcessorWaitTimeInMs(100L)
                  .segmentCleanupTimeIntervalInMs(100L)
                  .build()
```

To publish any event,
```java
eventStore.write(new Item("Item1", 20));
```

## Design:

Data storage is inspired from how kafka stores data for its partitions by creating smaller segments, and then creating index files for each segment for faster lookup of data.


