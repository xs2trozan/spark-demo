package com.deepak.spark;

/**
 * Model class for Spark Demo. This class consist three double values x, y, z 
 * where x, y represents input values and z is calculated value.
 * 
 * @author pathakd
 */
public class Data {

	private double x;
	private double y;
	private double z;

	/**
	 * Constructor
	 * @param x
	 * @param y
	 * @param z
	 */
	public Data(double x, double y, double z) {
		super();
		this.x = x;
		this.y = y;
		this.z = z;
	}

	public double getZ() {
		return z;
	}

	public double getY() {
		return y;
	}

	public double getX() {
		return x;
	}

	@Override
	public String toString() {
		return "Data [x=" + x + ", y=" + y + ", z=" + z + "]";
	}

}
