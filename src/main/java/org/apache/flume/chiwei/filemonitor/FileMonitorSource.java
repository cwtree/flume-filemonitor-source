package org.apache.flume.chiwei.filemonitor;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FileMonitorSource extends AbstractSource implements Configurable, EventDrivenSource {

	private static final Logger log = LoggerFactory.getLogger(FileMonitorSource.class);
	private ChannelProcessor channelProcessor;
	private RandomAccessFile monitorFile = null;
	private File coreFile = null;
	private long lastMod = 0L;
	private String monitorFilePath = null;
	private String positionFilePath = null;
	private FileChannel monitorFileChannel = null;
	private ByteBuffer buffer = ByteBuffer.allocate(1 << 20);// 1MB
	private long positionValue = 0L;
	private ScheduledExecutorService executor;
	private FileMonitorThread runner;
	private PositionLog positionLog = null;
	private Charset charset = null;
	private CharsetDecoder decoder = null;
	private CharBuffer charBuffer = null;
	private long counter = 0L;
	private Map<String, String> headers = new HashMap<String, String>();// event
																		// header
	private Object exeLock = new Object();
	private long lastFileSize = 0L;
	private long nowFileSize = 0L;

	@Override
	public synchronized void start() {
		channelProcessor = getChannelProcessor();
		executor = Executors.newSingleThreadScheduledExecutor();
		runner = new FileMonitorThread();
		executor.scheduleWithFixedDelay(runner, 500, 2000, TimeUnit.MILLISECONDS);
		super.start();
		log.debug("FileMonitorSource source started");
	}

	@Override
	public synchronized void stop() {
		positionLog.setPosition(positionValue);
		log.debug("Set the positionValue {} when stopped", positionValue);
		if (this.monitorFileChannel != null) {
			try {
				this.monitorFileChannel.close();
			} catch (IOException e) {
				log.error(this.monitorFilePath + " filechannel close Exception", e);
			}
		}
		if (this.monitorFile != null) {
			try {
				this.monitorFile.close();
			} catch (IOException e) {
				log.error(this.monitorFilePath + " file close Exception", e);
			}
		}
		executor.shutdown();
		try {
			executor.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			log.info("Interrupted while awaiting termination", ex);
		}
		executor.shutdownNow();
		super.stop();
		log.debug("FileMonitorSource source stopped");
	}

	@Override
	public void configure(Context context) {
		charset = Charset.forName("UTF-8");
		decoder = charset.newDecoder();
		this.monitorFilePath = context.getString(Constants.MONITOR_FILE);
		this.positionFilePath = context.getString(Constants.POSITION_DIR);
		Preconditions.checkArgument(monitorFilePath != null, "The file can not be null !");
		Preconditions.checkArgument(positionFilePath != null, "the positionDir can not be null !");
		if (positionFilePath.endsWith(":")) {
			positionFilePath += File.separator;
		} else if (positionFilePath.endsWith("\\") || positionFilePath.endsWith("/")) {
			positionFilePath = positionFilePath.substring(0, positionFilePath.length() - 1);
		}
		// create properties file when start the source if the properties is not
		// exists
		File file = new File(positionFilePath + File.separator + Constants.POSITION_FILE_NAME);
		if (!file.exists()) {
			try {
				file.createNewFile();
				log.debug("Create the {} file",Constants.POSITION_FILE_NAME);
			} catch (IOException e) {
				log.error("Create the position.properties error", e);
				return;
			}
		}
		try {
			coreFile = new File(monitorFilePath);
			lastMod = coreFile.lastModified();
		} catch (Exception e) {
			log.error("Initialize the File/FileChannel Error", e);
			return;
		}

		positionLog = new PositionLog(positionFilePath);
		try {
			positionValue = positionLog.initPosition();
		} catch (Exception e) {
			log.error("Initialize the positionValue in File positionLog", e);
			return;
		}
		lastFileSize = positionValue;
	}

	/**
	 * update the property value in time
	 * 
	 * @param prop
	 * @param value
	 */
	class FileMonitorThread implements Runnable {

		@Override
		public void run() {
			synchronized (exeLock) {
				log.debug("FileMonitorThread running ...");
				// coreFile = new File(monitorFilePath);
				long nowModified = coreFile.lastModified();
				// the file has been changed
				if (lastMod != nowModified) {
					log.debug(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>File modified ...");
					// you must record the last modified and now file size as
					// soon
					// as possible
					lastMod = nowModified;
					nowFileSize = coreFile.length();
					int readDataBytesLen = 0;
					try {
						log.debug("The Last coreFileSize {},now coreFileSize {}", lastFileSize,
								nowFileSize);
						// it indicated the file is rolled by log4j
						if (nowFileSize <= lastFileSize) {
							log.debug("The file size is changed to be lower,it indicated that the file is rolled by log4j.");
							positionValue = 0L;
						}
						lastFileSize = nowFileSize;
						monitorFile = new RandomAccessFile(coreFile, "r");
						// you must be instantiate the file channel Object when
						// the
						// file
						// changed
						monitorFileChannel = monitorFile.getChannel();
						monitorFileChannel.position(positionValue);
						// read file content into buffer
						int bytesRead = monitorFileChannel.read(buffer);
						// this while for it can not read all the data when the
						// file
						// modified
						while (bytesRead != -1) {
							log.debug("How many bytes read in this loop ? -->  {}", bytesRead);
							String contents = buffer2String(buffer);
							// every read,the last byte is \n,this can make sure
							// the
							// integrity of read data
							// include the \n
							int lastLineBreak = contents.lastIndexOf("\n") + 1;
							String readData = contents.substring(0, lastLineBreak);
							byte[] readDataBytes = readData.getBytes();
							readDataBytesLen = readDataBytes.length;
							positionValue += readDataBytesLen;
							// change the position value for next read
							monitorFileChannel.position(positionValue);
							log.debug("Read bytes {},Real read bytes {}", bytesRead,
									readDataBytesLen);
							headers.put(Constants.KEY_DATA_SIZE, String.valueOf(readDataBytesLen));
							headers.put(Constants.KEY_DATA_LINE,
									String.valueOf(readData.split("\n")));
							channelProcessor.processEvent(EventBuilder.withBody(readDataBytes,
									headers));
							// channelProcessor.processEventBatch(getEventByReadData(readData));
							log.debug("Change the next read position {}", positionValue);
							buffer.clear();
							bytesRead = monitorFileChannel.read(buffer);
						}
					} catch (Exception e) {
						log.error("Read data into Channel Error", e);
						log.debug("Save the last positionValue {} into Disk File", positionValue
								- readDataBytesLen);
						positionLog.setPosition(positionValue - readDataBytesLen);
					}
					counter++;
					if (counter % Constants.POSITION_SAVE_COUNTER == 0) {
						log.debug(
								Constants.POSITION_SAVE_COUNTER
										+ " times file modified checked,save the position Value {} into Disk file",
								positionValue);
						positionLog.setPosition(positionValue);
					}
				}
			}
		}

	}

	public List<Event> getEventByReadData(String readData) {
		String str[] = readData.split("\n");
		int len = str.length;
		List<Event> events = new ArrayList<Event>();
		for (int i = 0; i < len; i++) {
			Event event = EventBuilder.withBody((str[i]).getBytes());
			events.add(event);
		}
		return events;
	}

	public String buffer2String(ByteBuffer buffer) {
		buffer.flip();
		try {
			charBuffer = decoder.decode(buffer);
			return charBuffer.toString();
		} catch (Exception ex) {
			ex.printStackTrace();
			return "";
		}
	}

}
