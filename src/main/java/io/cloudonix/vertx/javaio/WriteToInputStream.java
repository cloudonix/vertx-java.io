package io.cloudonix.vertx.javaio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * A conversion utility to help move data from a Vert.x asynchronous stream to Java classic blocking IO.
 * 
 * Use this class to create a {@link WriteStream} that buffers data written to it so that a consumer
 * can use the {@link InputStream} API to read it.
 * 
 * The default queue size is 1000 writes, but it can be changed using {@link #setWriteQueueMaxSize(int)}
 * 
 * @author guss77
 */
public class WriteToInputStream extends InputStream implements WriteStream<Buffer>{
	
	private class PendingWrite {
		Buffer data;
		Promise<Void> completion;
		int position = 0;
		
		private PendingWrite(Buffer data, Promise<Void> completion) {
			this.data = data;
			this.completion = Objects.requireNonNullElse(completion, Promise.promise());
		}
		
		public boolean shouldDiscard() {
			if (data != null && position >= data.length()) {
				completion.tryComplete();
				if (everFull.compareAndSet(true, false))
					context.runOnContext(drainHandler::handle);
				return true;
			}
			return false;
		}
		
		public int available() {
			return data == null ? 0 : data.length();
		}
		
		public int readNext() {
			if (data == null) {
				this.completion.tryComplete();
				return -1;
			}
			int val = 0xFF & data.getByte(position++); // get byte's bitwise value, which is what InputStream#read() is supposed to return
			if (position > data.length())
				completion.tryComplete();
			return val;
		}
		
		public int read(byte[] b, int off, int len) {
			if (data == null || position >= data.length()) {
				completion.tryComplete();
				return data == null ? -1 : 0;
			}
			int max = Math.min(len, data.length() - position);
			data.getBytes(position, position + max, b, off);
			position += max;
			return max;
		}
	}

	private Handler<Void> drainHandler = __ -> {};
	private Handler<Throwable> errorHandler = t -> {};
	private volatile int maxSize = 1000;
	private volatile int maxBufferSize = Integer.MAX_VALUE;
	private ConcurrentLinkedQueue<PendingWrite> buffer = new ConcurrentLinkedQueue<>();
	private AtomicBoolean everFull = new AtomicBoolean();
	private volatile boolean closed = false;
	private ConcurrentLinkedQueue<CountDownLatch> readsWaiting = new ConcurrentLinkedQueue<>();
	private Context context;

	public WriteToInputStream(Vertx vertx) {
		context = vertx.getOrCreateContext();
	}
	
	public WriteToInputStream wrap(OutputStream os) {
		context.executeBlocking(p -> {
			try (os) {
				transferTo(os);
				p.complete();
			} catch (Throwable t) {
				p.fail(t);
			}
		}).onFailure(t -> {
			if (errorHandler != null)
				errorHandler.handle(t);
		});
		return this;
	}
	
	/* WriteStream stuff */
	
	@Override
	public WriteToInputStream drainHandler(Handler<Void> handler) {
		this.drainHandler = handler;
		return this;
	}

	@Override
	public void end(Handler<AsyncResult<Void>> handler) {
		// signal end of stream by writing a null buffer
		write(null, handler);
	}

	@Override
	public WriteToInputStream exceptionHandler(Handler<Throwable> handler) {
		// we don't have a way to propagate errors as we don't actually handle writing out and InputStream provides no feedback mechanism.
		errorHandler = handler;
		return this;
	}

	@Override
	public Future<Void> write(Buffer data) {
		if (closed)
			// accept all data and discard it, as unlike JDK9 Flow, we have no way to tell upstream to stop sending data
			return Future.succeededFuture();
		var promise = Promise.<Void>promise();
		if (data == null) // end of stream
			buffer.add(new PendingWrite(null, promise));
		else
			for (int start = 0; start < data.length();) {
				var buf = data.length() < maxBufferSize ? data : data.getBuffer(start, Math.min(data.length(), start + maxBufferSize));
				start += buf.length();
				buffer.add(new PendingWrite(buf, start < data.length() ? null : promise));
			}
		// flush waiting reads, if any
		for (var l = readsWaiting.poll(); l != null; l = readsWaiting.poll()) l.countDown();
		return promise.future();
	}

	@Override
	public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
		write(data).onComplete(handler);
	}

	@Override
	public WriteToInputStream setWriteQueueMaxSize(int maxSize) {
		this.maxSize = maxSize;
		return this;
	}
	
	public WriteToInputStream setMaxChunkSize(int maxSize) {
		maxBufferSize = maxSize;
		return this;
	}

	@Override
	public boolean writeQueueFull() {
		if (buffer.size() < maxSize)
			return false;
		everFull.compareAndSet(false, true);
		return true;
	}

	/* InputStream stuff */
	
	@Override
	synchronized public int read() throws IOException {
		while (true) {
			while (!buffer.isEmpty() && buffer.peek().shouldDiscard()) buffer.poll();
			if (!buffer.isEmpty())
				return buffer.peek().readNext();
			// set latch to signal we are waiting
			var latch = new CountDownLatch(1);
			readsWaiting.add(latch);
			if (buffer.isEmpty())
				try {
					latch.await();
				} catch (InterruptedException e) {
					throw new IOException("Failed to wait for data", e);
				}
			// now try to read again
		}
	}
	
	@Override
	synchronized public int read(byte[] b, int off, int len) throws IOException {
		if (b.length < off + len) // sanity first
			return 0;
		// we are going to be lazy here and not read more than one pending write, even if there are more available. The contract allows for that
		while (!buffer.isEmpty() && buffer.peek().shouldDiscard()) buffer.poll();
		if (!buffer.isEmpty())
			return buffer.peek().read(b, off, len);
		// we still wait if there is no data, but let read() implement the blocking
		int val = read();
		if (val < 0)
			return val;
		b[off] = (byte) (val & 0xFF);
		return 1 + Math.max(0, read(b, off + 1, len - 1));
	}

	@Override
	public int available() throws IOException {
		return buffer.stream().map(PendingWrite::available).reduce(0, (i,a) -> i += a);
	}
	
	@Override
	public void close() throws IOException {
		super.close();
		closed = true; // mark us closed, so that additional writes are NOPed
		// if we have any buffered data, flush it and trigger the write completions
		while (!buffer.isEmpty())
			buffer.poll().completion.tryComplete();
		// see if we need to call the drain handler to drain upstream
		if (everFull.compareAndSet(true, false))
			context.runOnContext(drainHandler::handle);
	}
}
