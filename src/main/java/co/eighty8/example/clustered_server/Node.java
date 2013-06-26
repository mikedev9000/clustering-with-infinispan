package co.eighty8.example.clustered_server;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;

public class Node {

	@Listener
	public class UserInputCacheListener {

		@CacheStarted
		public void handleStart(Event<String, String> event) {
			System.out.println("Cache Started... ");
		}

		@CacheStopped
		public void handleStop(Event<String, String> event) {
			System.out.println("Cache shudown.... ");
		}

		@CacheEntryCreated
		public void cacheEntryCreated(
				CacheEntryCreatedEvent<String, String> event) {
			System.out.println("Input added: " + event.getKey() + "=>"
					+ event.getValue());
		}

		@CacheEntryModified
		public void cacheEntryModified(
				CacheEntryModifiedEvent<String, String> event) {
			System.out.println("Input modified: " + event.getKey() + "=>"
					+ event.getValue());
		}

		@CacheEntryRemoved
		public void cacheEntryRemoved(
				CacheEntryRemovedEvent<String, String> event) {
			System.out.println("Input removed: " + event.getKey() + "=>"
					+ event.getValue());
		}
	}

	@Listener
	public class NodeStatusCacheListener {

		@CacheStarted
		public void handleStart(Event<String, String> event) {
			System.out.println("Cache Started... ");
		}

		@CacheStopped
		public void handleStop(Event<String, String> event) {
			System.out.println("Cache shudown.... ");
		}

		@CacheEntryCreated
		public void cacheEntryCreated(
				CacheEntryCreatedEvent<String, String> event) {
			System.out.println("Node added: " + event.getKey() + "=>"
					+ event.getValue());
		}

		@CacheEntryModified
		public void cacheEntryModified(
				CacheEntryModifiedEvent<String, String> event) {
			System.out.println("Node modified: " + event.getKey() + "=>"
					+ event.getValue());
		}

		@CacheEntryRemoved
		public void cacheEntryRemoved(
				CacheEntryRemovedEvent<String, String> event) {
			System.out.println("Node removed: " + event.getKey() + "=>"
					+ event.getValue());
		}
	}

	private class InputLoop implements Runnable {

		private BufferedReader buffer = new BufferedReader(
				new InputStreamReader(System.in));

		private String input;

		public void run() {

			System.out
					.println("Enter key, value pair for cache in this format: '$key=>$value'");

			while (true) {
				try {
					if (inputPresent()) {
						processInput();
					}
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
		}

		private boolean inputPresent() throws Throwable {
			input = buffer.readLine();
			return input != null && !input.isEmpty();
		}

		private void processInput() throws Throwable {
			String[] keyValue = input.split("=>");

			if (keyValue.length != 2) {
				System.out.println("Invalid input format!");
			} else {
				processKeyAndValue(keyValue[0], keyValue[1]);
			}
		}

		private void processKeyAndValue(String key, String value) {
			userInputCache.put(key, value);
		}

	}

	public static void main(String[] args) {
		Node node = new Node();
		try {
			node.initialize();
			node.run();
		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(1);
		}
	}

	private DefaultCacheManager cacheManager;
	private Cache<Object, Object> userInputCache;
	private Cache<Object, Object> nodeStatusCache;

	public Node() {
		cacheManager = new DefaultCacheManager();
	}

	public void initialize() throws Throwable {
		userInputCache = cacheManager.getCache("userInput");
		userInputCache.addListener(new UserInputCacheListener());

		nodeStatusCache = cacheManager.getCache("nodeStatus");
	}

	public void run() throws Throwable {
		startInputThread();
	}

	private void startInputThread() {
		Thread thread = new Thread(new InputLoop(), "InputLoop");
		thread.setDaemon(false);
		thread.start();
	}

}
