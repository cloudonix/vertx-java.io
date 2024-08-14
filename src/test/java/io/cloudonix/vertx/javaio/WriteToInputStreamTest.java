package io.cloudonix.vertx.javaio;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
class WriteToInputStreamTest {
	
	private final static Logger log = System.getLogger(WriteToInputStream.class.getName());

	@Test
	void test(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException {
		log.log(Level.INFO, "Starting test");
		var text = "hello world";
		var sink = new ByteArrayOutputStream();
		var cp = ctx.checkpoint();
		var stream = new WriteToInputStream(vertx);
		(new ReadStream<Buffer>() {
			private Handler<Void> endHandler;
			private Handler<Buffer> handler;

			@Override
			public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
				return this;
			}

			@Override
			public ReadStream<Buffer> handler(Handler<Buffer> handler) {
				this.handler = handler;
				return this;
			}

			@Override
			public ReadStream<Buffer> pause() {
				return this;
			}

			@Override
			public ReadStream<Buffer> resume() {
				vertx.setTimer(100, i -> {
					handler.handle(Buffer.buffer(text));
					endHandler.handle(null);
				});
				return this;
			}

			@Override
			public ReadStream<Buffer> fetch(long amount) {
				return this;
			}

			@Override
			public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
				this.endHandler = endHandler;
				return this;
			}
		}).pipeTo(stream).onFailure(ctx::failNow);
		vertx.<byte[]>executeBlocking(p -> {
			try {
				stream.transferTo(sink);
				stream.close();
				sink.close();
				p.complete(sink.toByteArray());
			} catch (IOException e) {
				p.fail(e);
			}
		}).onComplete(ctx.succeeding(res -> {
			assertThat(res, is(equalTo(text.getBytes())));
			cp.flag();
		}));
	}

	@Test
	void testLargeTransfer(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException {
		log.log(Level.INFO, "Starting testLargeTransfer");
		var text = "hello world";
		int count = 10000;
		var sink = new ByteArrayOutputStream();
		var cp = ctx.checkpoint();
		var stream = new WriteToInputStream(vertx);
		(new ReadStream<Buffer>() {
			private int remaining = count;
			private Handler<Void> endHandler;
			private Handler<Buffer> handler;

			@Override
			public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
				return this;
			}

			@Override
			public ReadStream<Buffer> handler(Handler<Buffer> handler) {
				this.handler = handler;
				return this;
			}

			@Override
			public ReadStream<Buffer> pause() {
				return this;
			}
			
			private void transfer() {
				if (remaining-- > 0) {
					handler.handle(Buffer.buffer(text));
					vertx.setTimer(1, i -> transfer());
				} else
					endHandler.handle(null);
			}

			@Override
			public ReadStream<Buffer> resume() {
				vertx.setTimer(100, i -> transfer());
				return this;
			}

			@Override
			public ReadStream<Buffer> fetch(long amount) {
				return this;
			}

			@Override
			public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
				this.endHandler = endHandler;
				return this;
			}
		}).pipeTo(stream).onSuccess(v -> {
		}).onFailure(ctx::failNow);
		vertx.<byte[]>executeBlocking(p -> {
			try {
				stream.transferTo(sink);
				stream.close();
				sink.close();
				p.complete(sink.toByteArray());
			} catch (IOException e) {
				p.fail(e);
			}
		}).onComplete(ctx.succeeding(res -> {
			var bres = Buffer.buffer(res);
			var test = text.getBytes();
			for (int i = 0; i < count; i++) {
				assertThat(bres.getBuffer(i * test.length, (i+1) * test.length).getBytes(), is(equalTo(test)));
			}
			cp.flag();
		}));
	}
	
	@Test
	public void testConvert(Vertx vertx, VertxTestContext ctx) throws IOException {
		log.log(Level.INFO, "Starting testConvert");
		var sink = new ByteArrayOutputStream();
		var result = Promise.<Void>promise();
		var os = new WriteToInputStream(vertx);
		os.wrap(sink).onComplete(result);
		os.end(Buffer.buffer("hello world"));
		result.future()
		.map(__ -> {
			log.log(Level.INFO, "Testing output stream result...");
			assertThat(sink.toByteArray(), is(equalTo("hello world".getBytes())));
			return null;
		})
		.onComplete(ctx.succeedingThenComplete())
		.onComplete(__ -> ctx.verify(os::close));
	}
	
	@Test
	public void testReChunkedWrites(Vertx vertx, VertxTestContext ctx) throws IOException {
		log.log(Level.INFO, "Starting testReChunkedWrites");
		var data = "hello world, this is a longish text which will be chunks";
		var sink = new ByteArrayOutputStream();
		var result = Promise.<Void>promise();
		var os = new WriteToInputStream(vertx);
		os.setMaxChunkSize(10).wrap(sink).onComplete(result);
		os.end(Buffer.buffer(data));
		result.future()
		.map(__ -> {
			log.log(Level.INFO, "Testing output stream result...");
			assertThat(sink.toByteArray(), is(equalTo(data.getBytes())));
			return null;
		})
		.onComplete(ctx.succeedingThenComplete())
		.onComplete(__ -> ctx.verify(os::close));
	}
		.onComplete(ctx.succeedingThenComplete());
	}

}
