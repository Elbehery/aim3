package de.tuberlin.dima.aim3.assignment3;

/**
 * Created by mustafa on 03/12/14.
 */
public class Matrix {

    private double[][] matrix = {
            {1, 2, 3, 4, 5},
            {1, 2, 3, 4, 5},
            {1, 2, 3, 4, 5},
            {1, 2, 3, 4, 5},
            {1, 2, 3, 4, 5}
    };


    public Matrix() {

    }

    public Matrix(int row, int col) {

        this.matrix = new double[row][col];
    }

    public Matrix(double[][] matrix) {

        this.matrix = matrix;
    }

    public double[][] getMatrix() {
        return matrix;
    }

    public void setMatrix(double[][] matrix) {
        this.matrix = matrix;
    }


    public double traceMatrix(Matrix m) {

        int rows = m.getMatrix().length;
        int cols = m.getMatrix()[0].length;
        double sum = 0;

        if (rows == cols) {
            for (int i = 0; i < rows; i++)
                sum += m.getMatrix()[i][i];
        } else {
            throw new IllegalArgumentException("Trace is defined for SQAURE MATRICES ONLY -- This matrix is NOT SQUARE");
        }
        return sum;
    }


    // writing the matrix with its trace value
    public String toString() {

        StringBuffer output = new StringBuffer();
        int rows = this.getMatrix().length;
        int cols = this.getMatrix()[0].length;

        // writing the matrix
        output.append("{\n");
        for (int i = 0; i < rows; i++) {

            output.append("{");

            for (int j = 0; j < cols; j++) {
                output.append(this.getMatrix()[i][j]);
                if (j != cols - 1) {
                    output.append(",");
                }
            }
            output.append("},\n");
        }

        output.append("}");

        // writing the trace of the matrix
        double trace = 0;

        try {
            trace = traceMatrix(this);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        if (trace != 0) {
            output.append("         Matrix Trace = ").append(Double.toString(trace));
        } else {
            output.append("Trace is defined for SQAURE MATRICES ONLY -- This matrix is NOT SQUARE");
        }


        return output.toString();

    }


}
