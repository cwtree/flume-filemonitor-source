package org.apache.flume.chiwei.filemonitor;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PositionLog {

	private static final Logger log = LoggerFactory
			.getLogger(PositionLog.class);
	
	private FileChannel positionFileChannel;
	private String postionFilePath;
	private RandomAccessFile raf = null;
	private String filePath = null;
	public FileChannel getPositionFileChannel() {
		return positionFileChannel;
	}
	public void setPositionFileChannel(FileChannel positionFileChannel) {
		this.positionFileChannel = positionFileChannel;
	}
	public String getPostionFilePath() {
		return postionFilePath;
	}
	public void setPostionFilePath(String postionFilePath) {
		this.postionFilePath = postionFilePath;
	}
	
	public PositionLog() {
	}
	public PositionLog(String postionFilePath) {
		this.postionFilePath = postionFilePath;
	}
	public long initPosition() throws Exception {
		filePath = postionFilePath + File.separator
				+ Constants.POSITION_FILE_NAME;
		File file = new File(filePath);
		if (!file.exists()) {
			try {
				file.createNewFile();
				log.debug("Create the position file");
			} catch (IOException e) {
				log.error("Create the position error", e);
				throw e;
			}
		}
		try {
			raf = new RandomAccessFile(filePath, "rw");
			this.positionFileChannel =raf.getChannel();
			long fileSize = positionFileChannel.size();
			if(fileSize==0) {
				log.debug("The file content is null,init the value 0");
				ByteBuffer buffer = ByteBuffer.allocate(1);
				buffer.put("0".getBytes());
				buffer.flip();
				positionFileChannel.write(buffer);
				raf.close();
				return 0L;
			}else {
				return getPosition();
			}
		} catch (Exception e) {
			log.error("Init the position file error",e);
			throw e;
		} 
	}
	
	public long getPosition() {
		try {
			raf = new RandomAccessFile(filePath, "rw");
			this.positionFileChannel =raf.getChannel();
			long fileSize = positionFileChannel.size();
			ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
			int bytesRead = positionFileChannel.read(buffer);
			StringBuffer sb = new StringBuffer();
			while(bytesRead!=-1) {
				buffer.flip();
				while(buffer.hasRemaining()) {
					sb.append((char)buffer.get());
				}
				buffer.clear();
				bytesRead = positionFileChannel.read(buffer);
			}
			raf.close();
			return Long.parseLong(sb.toString());
		} catch (Exception e) {
			log.error("Get Position Value Error",e);
			return -1;
		}
	}
	
	public long setPosition(Long position) {
		try {
			raf = new RandomAccessFile(filePath, "rw");
			this.positionFileChannel =raf.getChannel();
			String positionStr = String.valueOf(position);
			int bufferSize = positionStr.length();
			ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
			buffer.clear();
			buffer.put(positionStr.getBytes());
			buffer.flip();
			while(buffer.hasRemaining()) {
				this.positionFileChannel.write(buffer);
			}
			raf.close();
			log.debug("Set Position Value Successfully {}",position);
			return position;
		} catch (Exception e) {
			log.error("Set Position Value Error",e);
			return -1;
		}
	}
	
}
