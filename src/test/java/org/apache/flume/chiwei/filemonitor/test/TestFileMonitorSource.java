package org.apache.flume.chiwei.filemonitor.test;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.chiwei.filemonitor.FileMonitorSource;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestFileMonitorSource {

	private FileMonitorSource source;
	private Context context;
	private Channel channel;
	private ChannelSelector rcs = new ReplicatingChannelSelector();
	
	@Before
	public void before() {
		this.context = new Context();
		this.context.put("file", TestConstants.FILE);
		this.context.put("positionDir", TestConstants.POSITION_DIR);
	
		source = new FileMonitorSource();
		channel = new MemoryChannel();
		rcs.setChannels(Lists.newArrayList(channel));
		source.setChannelProcessor(new ChannelProcessor(rcs));
	}
	
	@Test
	public void test() {
		//source.configure(context);
		//source.start();
	}
	
}


























