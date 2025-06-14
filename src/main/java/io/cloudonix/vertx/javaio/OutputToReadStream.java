package io.cloudonix.vertx.javaio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * A conversion utility to help move data from a Java classic blocking IO to a Vert.x asynchronous Stream.
 * 
 * Use this class to create an {@link OutputStream} that pushes data written to it to a {@link ReadStream}
 * API.
 *  
 * The ReadStream handlers will be called on a Vert.x context, and the {@link #close()} method must be called
 * for the ReadStream end handler to be triggered.
 * 
 * It is recommended to use this class in the context of a blocking try-with-resources block, to
 * ensure that streams are closed properly. For example:
 * 
 * <tt><pre>
 * try (final OutputToReadStream os = new OutputToReadStream(vertx); final InputStream is = getInput()) {
 *   os.pipeTo(someWriteStream);
 *   is.transferTo(os);
 * }
 * </pre></tt>
 * 
 * @author guss77
 */
public class OutputToReadStream extends OutputStream implements ReadStream<Buffer> {
	
	private AtomicReference<CountDownLatch> paused = new AtomicReference<>(new CountDownLatch(0));
	private boolean closed;
	private AtomicLong demand = new AtomicLong(0);
	private Handler<Void> endHandler = v -> {};
	private Handler<Buffer> dataHandler = d -> {};
	private Handler<Throwable> errorHandler = t -> {};
	private Context context;
	
	public OutputToReadStream(Vertx vertx) {
		context = vertx.getOrCreateContext();
	}
	
	/**
	 * Helper utility to pipe a Java {@link InputStream} to a {@link WriteStream}.
	 * 
	 * This method is non-blocking and Vert.x context safe. It uses the common ForkJoinPool to perform the
	 * Java blocking IO and will try to propagate IO failures to the returned {@link Future}.
	 * 
	 * This method uses {@link InputStream#transferTo(OutputStream)} to copy all the data, and will then
	 * attempt to close both streams asynchronously. Some Java compilers might not detect that the streams
	 * will be safely closed and will issue leak warnings.
	 * 
	 * @param source InputStream to drain
	 * @param sink WriteStream to pipe data to
	 * @return a Future that will succeed when all the data have been written and the streams closed, or fail if an
	 * {@link IOException} has occurred
	 */
	public Future<Void> pipeFromInput(InputStream source, WriteStream<Buffer> sink) {
		Promise<Void> promise = Promise.promise();
		pipeTo(sink, promise);
		ForkJoinPool.commonPool().submit(() -> {
			try (final InputStream is = source; final OutputStream os = this){
				source.transferTo(this);
			} catch (IOException e) {
				promise.tryFail(e);
			}
		});
		return promise.future();
	}
	
	/**
	 * Helper utility to pipe a Java {@link InputStream} to a {@link WriteStream}.
	 * 
	 * This method is non-blocking and Vert.x context safe. It uses the common ForkJoinPool to perform the
	 * Java blocking IO and will try to propagate IO failures to the returned {@link Future}
	 * 
	 * This method uses {@link InputStream#transferTo(OutputStream)} to copy all the data, and will then
	 * attempt to close both streams asynchronously. Some Java compilers might not detect that the streams
	 * will be safely closed and will issue leak warnings.
	 * 
	 * @param source InputStream to drain
	 * @param sink WriteStream to pipe data to
	 * @param handler a handler that will be called when all the data have been written and the streams closed,
	 * or if an {@link IOException} has occurred.
	 */
	public void pipeFromInput(InputStream source, WriteStream<Buffer> sink, Handler<AsyncResult<Void>> handler) {
		pipeFromInput(source, sink).onComplete(handler);
	}
	
	/**
	 * Propagate an out-of-band error (likely generated or handled by the code that feeds the output stream) to the end of the 
	 * read stream to let them know that the result is not going to be good.
	 * @param t error to be propagated down the stream
	 */
	public void sendError(Throwable t) {
		context.executeBlocking(p -> {
			try {
				errorHandler.handle(t);
			} finally {
				p.tryComplete();
			}
		});
	}
	
	/* ReadStream stuff */
	
	@Override
	public OutputToReadStream exceptionHandler(Handler<Throwable> handler) {
		// we are usually not propagating exceptions as OutputStream has no mechanism for propagating exceptions down,
		// except when wrapping an input stream, in which case we can forward InputStream read errors to the error handler.
		errorHandler = Objects.requireNonNullElse(handler, t -> {});
		return this;
	}

	@Override
	public OutputToReadStream handler(Handler<Buffer> handler) {
		this.dataHandler = Objects.requireNonNullElse(handler, d -> {});
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
		this.endHandler = Objects.requireNonNullElse(endHandler, v -> {});
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
		if (closed)
			return;
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
			} catch (Throwable t) {
				errorHandler.handle(t);
			} finally {
				awaiter.countDown();
			}
		});
		try {
			awaiter.await();
		} catch (InterruptedException e) { }
	}
	
}
