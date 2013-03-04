package motion;

public class FrameObject {
	private int camNumber;
	private int frameNumber;
	private byte[] data;
	
	public int getCamNumber() {
		return camNumber;
	}

	public void setCamNumber(int camNumber) {
		this.camNumber = camNumber;
	}

	public int getFrameNumber() {
		return frameNumber;
	}

	public void setFrameNumber(int frameNumber) {
		this.frameNumber = frameNumber;
	}

	public byte[] getData() {
		return data;
	}

	public FrameObject(int camNumber, int frameNumber, byte[] data) {
		super();
		this.camNumber = camNumber;
		this.frameNumber = frameNumber;
		this.data = data;
	}
}
