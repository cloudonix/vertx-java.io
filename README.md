# vertx-java.io
A set of utilities for integrating Vert.x IO with Java IO.

## Usage

In your `pom.xml` add the Jitpack repository:

```xml
<repositories>
	<repository>
    	<id>jitpack.io</id>
        <url>https://jitpack.io</url>
	</repository>
</repositories>
```

Then add a dependency on this library:
```xml
<dependency>
    <groupId>com.github.cloudonix</groupId>
    <artifactId>vertx-java.io</artifactId>
    <version>main</version>
</dependency>
```

## Features

### Connect Vert.x Byte Buffer Streams to Java Input/Output Streams

You need to read from a [Vert.x IO stream](https://vertx.io/docs/vertx-core/java/#streams) and write into a Java `OutputStream`, or read from a Java `InputStream` and write into a Vert.x IO stream, when these utility classes are what you need:

* `OutputToReadStream` is an implementation of a Java `OutputStream` and a Vert.x `ReadStream<Buffer>` - data written  to the `OutputStream` API can be received from the `ReadStream<Buffer>` API.

* `WriteToInputStream` is an implementation of a Java `InputStream` and a Vert.x `WriteStream<Buffer>` - anything written to the `WriteStream<Buffer>` API can be read from the `InputStream` API.

In all cases, both Java's blocking semantics are observed (using the thread in which the blocking Java stream operation is executed) as well as Vert.x non-blocking API (using `Context.runOnContext()` to execute Vert.x callbacks).

Using these classes and Vert.x and Java IO utility methods, it is also trivial to convert a Java `InputStream` to a Vert.x `ReadStream<Buffer>` or Java `OutputStream` to a Vert.x `WriteStream<Buffer>` and vice-versa. A minimal implementation is available as `OutputToReadStream.wrap(InputStream)` and `WriteInputStream.wrap(OutputStream)`  - see the unit tests for usage examples and the source code as an example that you can customize for your needs.

