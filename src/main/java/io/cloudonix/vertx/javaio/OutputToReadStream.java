package io.cloudonix.vertx.javaio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

public class OutputToReadStream extends OutputStream implements ReadStream<Buffer> {
	
	private AtomicReference<CountDownLatch> paused = new AtomicReference<>(new CountDownLatch(0));
	private boolean closed;
	private AtomicLong demand = new AtomicLong(0);
	private Handler<Void> endHandler;
	private Handler<Buffer> dataHandler;
	private Handler<Throwable> errorHandler;
	private Context context;
	
	public OutputToReadStream(Vertx vertx) {
		context = vertx.getOrCreateContext();
	}
	
	/**
	 * Helper utility to "convert" a Java {@link InputStream} to a {@link ReadStream}<code>&lt;Buffer&gt;</code>.
	 * 
	 * This method uses {@link InputStream#transferTo(OutputStream)}, and in addition it will attempt to close the
	 * InputStream once it is done transferring its data, and call this ReadStream's end handler if no error occurred.
	 * 
	 * If an error has occurred, the end handler is not guaranteed to be called, and a "best effort" attempt will be made
	 * to close the input stream.
	 * @param is InputStream instance to wrap
	 * @return this instance, which will immediately start producing data from the specified input stream
	 */
	public OutputToReadStream wrap(InputStream is) {
		context.executeBlocking(p -> {
			try (is) {
				is.transferTo(this);
				p.complete();
			} catch (IOException e) {
				p.fail(e);
			}
		}).onFailure(t -> {
			if (errorHandler != null)
				errorHandler.handle(t);
		}).onSuccess(v -> {
			if (endHandler != null)
				endHandler.handle(null);
		});
		return this;
	}
	
	/* ReadStream stuff */
	
	@Override
	public OutputToReadStream exceptionHandler(Handler<Throwable> handler) {
		// we are usually not propagating exceptions as OutputStream has no mechanism for propagating exceptions down,
		// except when wrapping an input stream, in which case we can forward InputStream read errors to the error handler.
		errorHandler = handler;
		return this;
	}

	@Override
	public OutputToReadStream handler(Handler<Buffer> handler) {
		this.dataHandler = handler;
		return this;
	}

	@Override
	public OutputToReadStream pause() {
		paused.getAndSet(new CountDownLatch(1)).countDown();
		return this;
	}

	@Override
	public OutputToReadStream resume() {
		paused.getAndSet(new CountDownLatch(0)).countDown();
		return this;
	}

	@Override
	public OutputToReadStream fetch(long amount) {
		resume();
		demand.addAndGet(amount);
		return null;
	}

	@Override
	public OutputToReadStream endHandler(Handler<Void> endHandler) {
		this.endHandler = endHandler;
		return this;
	}

	/* OutputStream stuff */
	
	@Override
	synchronized public void write(int b) throws IOException {
		if (closed)
			throw new IOException("OutputStream is closed");
		try {
			paused.get().await();
		} catch (InterruptedException e) {
			throw new IOException("Interrupted a wait for stream to resume", e);
		}
		push(Buffer.buffer(1).appendByte((byte) (b & 0xFF)));
	}
	
	@Override
	synchronized public void write(byte[] b, int off, int len) throws IOException {
		if (closed)
			throw new IOException("OutputStream is closed");
		try {
			paused.get().await();
		} catch (InterruptedException e) {
			throw new IOException("Interrupted a wait for stream to resume", e);
		}
		push(Buffer.buffer(len - off).appendBytes(b, off, len));
	}

	@Override
	synchronized public void close() throws IOException {
		closed = true;
		try {
			paused.get().await();
		} catch (InterruptedException e) {
			throw new IOException("Interrupted a wait for stream to resume", e);
		}
		push(null);
	}
	
	/* Internal implementation */
	
	private void push(Buffer data) {
		var awaiter = new CountDownLatch(1);
		context.runOnContext(v -> {
			try {
				if (data == null) // end of stream
					endHandler.handle(null);
				else
					dataHandler.handle(data);
				awaiter.countDown();
			} catch (Throwable t) {
				if (errorHandler != null)
					errorHandler.handle(t);
				else
					System.err.println("Unexpected exception in OutputToReadStream and no error handler: " + t);
			}
		});
		try {
			awaiter.await();
		} catch (InterruptedException e) { }
	}
	
}
