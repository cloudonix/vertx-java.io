package io.cloudonix.vertx.javaio;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
class OutputToReadStreamTest {

	@Test
	void test(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException {
		var text = "hello world";
		var source = new ByteArrayInputStream(new String(text).getBytes());
		var testBuffer = Buffer.buffer();
		var cp = ctx.checkpoint();
		var stream = new OutputToReadStream(vertx);
		stream.pipeTo(new WriteStream<Buffer>() {
			
			@Override
			public boolean writeQueueFull() {
				return false;
			}
			
			@Override
			public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
				write(data).onComplete(handler);
			}
			
			@Override
			public Future<Void> write(Buffer data) {
				testBuffer.appendBuffer(data);
				return Future.succeededFuture();
			}
			
			@Override
			public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
				return this;
			}
			
			@Override
			public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
				return this;
			}
			
			@Override
			public void end(Handler<AsyncResult<Void>> handler) {
				cp.flag();
				handler.handle(Future.succeededFuture());
			}
			
			@Override
			public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
				return this;
			}
		});
		source.transferTo(stream);
		source.close();
		stream.close();
		ctx.awaitCompletion(3, TimeUnit.SECONDS);
		assertThat(testBuffer.toString(), is(equalTo(text)));
	}


	@Test
	void testLargeTransfer(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException {
		var text = "hello world".getBytes();
		int count = 10000;
		var inputbuf = new byte[count * text.length];
		for (int i = 0; i < count; i++)
			System.arraycopy(text, 0, inputbuf, i * text.length, text.length);
		var source = new ByteArrayInputStream(inputbuf);
		var testBuffer = Buffer.buffer();
		var cp = ctx.checkpoint();
		var stream = new OutputToReadStream(vertx);
		stream.pipeTo(new WriteStream<Buffer>() {
			
			@Override
			public boolean writeQueueFull() {
				return false;
			}
			
			@Override
			public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
				write(data).onComplete(handler);
			}
			
			@Override
			public Future<Void> write(Buffer data) {
				testBuffer.appendBuffer(data);
				return Future.succeededFuture();
			}
			
			@Override
			public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
				return this;
			}
			
			@Override
			public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
				return this;
			}
			
			@Override
			public void end(Handler<AsyncResult<Void>> handler) {
				cp.flag();
				handler.handle(Future.succeededFuture());
			}
			
			@Override
			public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
				return this;
			}
		});
		source.transferTo(stream);
		source.close();
		stream.close();
		ctx.awaitCompletion(30, TimeUnit.SECONDS);
		assertThat(testBuffer.getBytes(), is(equalTo(inputbuf)));
	}

	@Test
	public void testConvert(Vertx vertx, VertxTestContext ctx) throws InterruptedException, IOException {
		var cp = ctx.checkpoint();
		var source = new ByteArrayInputStream("hello world".getBytes());
		var testBuffer = Buffer.buffer();
		try (final var os = new OutputToReadStream(vertx); final var is = source) {
			os.exceptionHandler(ctx::failNow).pipeTo(new WriteStream<Buffer>() {
				@Override
				public boolean writeQueueFull() {
					return false;
				}
				
				@Override
				public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
					write(data).onComplete(handler);
				}
				
				@Override
				public Future<Void> write(Buffer data) {
					testBuffer.appendBuffer(data);
					return Future.succeededFuture();
				}
				
				@Override
				public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
					return this;
				}
				
				@Override
				public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
					return this;
				}
				
				@Override
				public void end(Handler<AsyncResult<Void>> handler) {
					cp.flag();
					handler.handle(Future.succeededFuture());
				}
				
				@Override
				public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
					return this;
				}
			});
			is.transferTo(os);
		}
		ctx.awaitCompletion(3, TimeUnit.SECONDS);
		assertThat(testBuffer.toString(), is(equalTo("hello world")));
	}
}
