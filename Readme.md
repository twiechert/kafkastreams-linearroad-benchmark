# Linear Road Benchmark - Kafka Streams Implementation

> A full implementation of the [Linear Road Benchmark](http://www.isys.ucl.ac.be/vldb04/eProceedings/contents/pdf/RS12P1.PDF) using **Apache Kafka Streams**, featuring real-time toll calculation, accident detection, and vehicle tracking on simulated expressways.

**Keywords:** Kafka Streams, Linear Road, stream processing, real-time analytics, toll computation, CEP, complex event processing, benchmark, Apache Kafka, KTable, KStream, event-driven architecture

---

## Overview

The Linear Road Benchmark is a well-known streaming benchmark that simulates a toll system for a set of expressways. Vehicles emit position reports every 30 seconds, and the system must respond in real time with toll notifications, accident alerts, and account balance queries.

This project implements all required benchmark components using the Kafka Streams DSL, including custom windowing strategies and event-time processing. The detailed design rationale is documented below and also available as a [PDF](design-document.pdf).

## Getting Started

### Starting the Application

For most configuration items there are default values specified which can be overridden. As the cluster profile is set to active, the `application-cluster.properties` file has precedence and overrides the default values specified in `application.properties`. If you execute the program as shown below, all stream tuples of the NOV and LAV stream will be printed to stdout.

```bash
java -jar apps/kafka-streams-linearroad.jar \
  --linearroad.data.path=/home/hadoop/linearroad/L1/car.dat \
  --linearroad.hisotical.data.path=/home/hadoop/linearroad/L1/car.dat.tolls.dat \
  --linearroad.kafka.bootstrapservers=localhost:9092 \
  --linearroad.mode.debug=NOV,LAV \
  --spring.profiles.active=cluster
```

If the historical toll table has already been fed, you can skip this part:

```bash
java -jar apps/kafka-streams-linearroad.jar \
  --linearroad.data.path=/home/hadoop/linearroad/L1/car.dat \
  --linearroad.hisotical.data.path=/home/hadoop/linearroad/L1/car.dat.tolls.dat \
  --linearroad.kafka.bootstrapservers=localhost:9092 \
  --linearroad.mode=no-historial-feed \
  --linearroad.mode.debug=NOV,LAV \
  --spring.profiles.active=cluster
```

### Building the Application

```bash
mvn clean package -Dmaven.test.skip=true
```

## Serializer Libraries

The benchmark is currently configured to use the [fast-serialization](https://github.com/RuedigerMoeller/fast-serialization) library, because in our conducted experiments it was the fastest and most space-efficient library from those we tested. You may replace this library -- this repository already integrates Serde implementations for Jackson (with Smile addon) and Kryo, all located in `core.serde.provider`. To change the library system-wide, simply make the class `core.serde.DefaultSerde` extend a different Serde implementation.

---

## Design Document: Stream and Table Components

The following section summarizes all the streams and tables that are generated to realize the benchmark. Each component is described with its input/output types and the implementation rationale.

### 1. Toll History Table

| Property | Value |
|---|---|
| Input stream/topic | Toll History Topic |
| Implemented in | `stream/historical/table/TollHistoryTableBuilder` |
| Output Type | Table |
| Output key | `XwayVehicleDay` |
| Output value | `Double` |

In order to respond to the daily expenditure request, we need the historical information of total expenditures per day, xway and vehicle. These historical information were generated in preparation and have to be made available to the application. The information is materialized to a table (KTable) which is created and fed before the actual benchmark starts. During runtime, the stream of daily expenditure request is joined with this table. As the table is partitioned by the tuple key, the operators will perform only local table lookups to respond to the queries.

### 2. Current Expenditure per Vehicle Table

| Property | Value |
|---|---|
| Input stream/topic | Segment Crossing Position Report Stream, Current Toll Stream |
| Implemented in | `stream/historical/table/CurrentExpenditurePerVehicleTableBuilder` |
| Output Type | Table |
| Output key | `Integer` |
| Output value | `ExpenditureAt` |

This table is dynamically updated and holds the summed expenditures per vehicle. It is required to respond to the type 2 queries and is generated based on the stream of current tolls (keyed by *(xway, vehicle, day)*) and the segment crossing position reports. A toll is charged only once per segment, so the reduced segment crossing position report stream is used as input. The toll assessment is asynchronous to the toll notification -- the toll is assessed after leaving the respective segment.

When a position report of segment *s* and minute *m* arrives, we need to assess the toll that was valid at *s-1* at the point in time the driver entered segment *s-1*. The segment crossing position report *p* and the current toll stream are joined after re-keying *p* such that segment *s* becomes *s-1*. The joined streams are re-keyed to vehicle id only, yielding a stream of accountable tolls per vehicle. An unwindowed aggregation sums these tolls into a materialized KTable.

### 3. Position Report Stream

| Property | Value |
|---|---|
| Input stream/topic | PositionReport Topic |
| Implemented in | `stream/PositionReportStreamBuilder` |
| Output Type | Stream |
| Output key | `XwaySegmentDirection` |
| Output value | `PositionReport` |

The stream of position reports is the starting point, used to determine average velocities in a certain segment or the number of vehicles in a certain time window. The application reads from a Kafka topic that is fed after deployment and start of the stream topology. The stream is keyed per *(xway, segment, direction)*, which makes it directly reusable for the NOV, LAV, and Accident Detection streams.

### 4. Segment Crossing Position Report Stream

| Property | Value |
|---|---|
| Input stream/topic | PositionReport Topic |
| Implemented in | `stream/SegmentCrossingPositionReportBuilder` |
| Output Type | Stream |
| Output key | `VehicleIdXwayDirection` |
| Output value | `SegmentCrossing` |

A toll is only charged if the driver has crossed a segment -- multiple position reports in the same segment must not cause multiple charges. This stream is built from the position report stream, re-keyed to *(vehicleId, xway, direction)* tuples, then reduced such that only the first position report within one segment is preserved.

To achieve this reduction, the re-keyed position report stream *p1* is duplicated with event times shifted by +30s in the duplicate *p2*. Then *p1* is left-joined with *p2* and the element from *p1* is emitted if `segment(p1) != segment(p2)`.

The toll for segment *s* is assessed when the driver enters segment *s+1*. To carry the entry time of the preceding segment, an unwindowed pairwise aggregation connects each data-item with its predecessor:

```java
.aggregateByKey(() -> new SegmentCrossing(),
    (key, value, agg) -> new SegmentCrossing(value, agg.getTime()))
.toStream();
```

### 5. Daily Expenditure Request Stream

| Property | Value |
|---|---|
| Input stream/topic | DailyExpenditure Topic |
| Implemented in | `stream/DailyExpenditureRequestStreamBuilder` |
| Output Type | Stream |
| Output key | `DailyExpenditureRequest` |
| Output value | `Void` |

Reads from the respective Kafka topic and provides daily expenditure requests as a stream. Not materialized directly because it is re-keyed to create the corresponding response stream.

### 6. Daily Expenditure Response Stream

| Property | Value |
|---|---|
| Input stream/topic | Daily Expenditure Request Stream, Toll History Table |
| Implemented in | `stream/DailyExpenditureResponseStreamBuilder` |
| Output Type | Stream |
| Output key | `Void` |
| Output value | `DailyExpenditureResponse` |

The request stream is left-joined with the toll history table. The request stream is re-keyed to *(xway, vehicle, day)* tuples for the join. If the toll table does not contain the respective entry, a toll of 0 is responded.

### 7. Account Balance Request Stream

| Property | Value |
|---|---|
| Input stream/topic | AccountBalanceRequest Topic |
| Implemented in | `stream/AccountBalanceRequestStreamBuilder` |
| Output Type | Stream |
| Output key | `AccountBalanceRequest` |
| Output value | `Void` |

Reads from the respective Kafka topic created from input tuples of query type 2. Used as the trigger stream for the account balance response stream.

### 8. Account Balance Response Stream

| Property | Value |
|---|---|
| Input stream/topic | Account Balance Request Stream, Current Expenditure per Vehicle Table |
| Implemented in | `stream/historical/AccountBalanceResponseStreamBuilder` |
| Output Type | Stream |
| Output key | `Void` |
| Output value | `AccountBalanceResponse` |

Represents the response stream for type 2 queries. The request stream is re-keyed to the vehicle id, materialized, and joined with the Current Expenditure per Vehicle Table to generate response messages.

### 9. Number of Vehicles Stream

| Property | Value |
|---|---|
| Input stream/topic | PositionReport Stream |
| Implemented in | `stream/NumberOfVehiclesStreamBuilder` |
| Output Type | Stream |
| Output key | `XwaySegmentDirection` |
| Output value | `NumberOfVehicles` |

Counts the number of distinct vehicles per *(xway, segment, direction)* tuple in a one-minute tumbling window (event time). The windowed position report is aggregated by storing observed vehicle identifiers in a set, then determining the set size.

### 10. Latest Average Velocity Stream

| Property | Value |
|---|---|
| Input stream/topic | PositionReport Topic |
| Implemented in | `stream/LatestAverageVelocityStreamBuilder` |
| Output Type | Stream |
| Output key | `XwaySegmentDirection` |
| Output value | `AverageVelocity` |

Emits every minute the latest average velocity of vehicles in the last five minutes per *(xway, segment, direction)* tuple. Uses a sliding window of 300 seconds with a 60-second advance.

A custom time window implementation handles the first five minutes: for all timestamps *t < 300s*, the smaller initial windows are also returned so that tolls can be calculated for minutes 2, 3, and 4.

The average is calculated iteratively using: `avg_n = avg_(n-1) * (n-1)/n + x_n/n`

### 11. Accident Detection Stream

| Property | Value |
|---|---|
| Input stream/topic | PositionReport Stream |
| Implemented in | `stream/AccidentDetectionStreamBuilder` |
| Output Type | Stream |
| Output key | `XwaySegmentDirection` |
| Output value | `Long` |

The current toll depends on accident occurrences. Two or more vehicles in the same segment, lane, and position being stopped in at least four subsequent reports constitute an accident.

The stream is re-keyed to *(xway, segment, direction, lane, position)* tuples and windowed with a 120-second window and 30-second slide. The pairwise aggregation stores vehicle ids in a map with occurrence counts. A filter checks for at least two entries with counts equal to four.

Accidents at segment *s* are flat-mapped to segments *s, s-1, s-2, s-3, s-4* so that upstream vehicles and toll calculations can account for them.

### 12. Accident Notification Stream

| Property | Value |
|---|---|
| Input stream/topic | Accident Detection Stream, Segment Crossing Position Report Stream |
| Implemented in | `stream/AccidentNotificationStreamBuilder` |
| Output Type | Stream |
| Output key | `Void` |
| Output value | `AccidentNotification` |

Joins detected accidents with segment crossing position reports to notify affected drivers. The segment crossing stream (not the simple position report stream) is used because a position report can only trigger a notification if the preceding report was from a different segment.

The notification timing is enforced by subtracting one minute from the position report stream's event time, ensuring that a position report at minute *m* does not trigger a notification for an accident that occurred at minute *m*.

### 13. Current Toll Stream

| Property | Value |
|---|---|
| Input stream/topic | Accident Detection Stream, Latest Average Velocity Stream, Number of Vehicles Stream |
| Implemented in | `stream/CurrentTollStreamBuilder` |
| Output Type | Stream |
| Output key | `XwaySegmentDirection` |
| Output value | `CurrentToll` |

The toll for a given *(xway, segment, direction)* tuple depends on: accident occurrence downstream, number of distinct vehicles in the past minute, and latest average velocity over the last five minutes. The toll calculated for observations at minute *m* is declared valid for minute *m+1*, avoiding the need to shift event times.

The LAV stream is joined with the NOV stream, then left-joined with the accident detection stream (left-join because accidents don't emit every minute). A subsequent map operation applies the toll formula.

### 14. Toll Notification Stream

| Property | Value |
|---|---|
| Input stream/topic | Current Toll Stream, Segment Crossing Position Report Stream |
| Implemented in | `stream/TollNotificationStreamBuilder` |
| Output Type | Stream |
| Output key | `Void` |
| Output value | `TollNotification` |

Notifies drivers about the current toll immediately after entering a segment. Uses the Segment Crossing Position Report Stream as the trigger (ensuring at most one notification per segment). A filter excludes vehicles on exit lanes, which are toll-free.
