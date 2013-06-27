package co.eighty8.example.clustered_server;

public class Three {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Node node = new Node(3);
		try {
			node.initialize();
			node.run();
		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(1);
		}
	}

}
