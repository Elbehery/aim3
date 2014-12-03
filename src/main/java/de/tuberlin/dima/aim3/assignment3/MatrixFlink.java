package de.tuberlin.dima.aim3.assignment3;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by mustafa on 03/12/14.
 */
public class MatrixFlink {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //TODO : Example of non working input matrix. The dimensions are   (5*3), so there is no trace and the multiplication will not work with the default matrix in one of the cases.
        //TODO However, all the exception are handled and printed
        //DataSource<Matrix> matrixDataSet1 = env.fromElements(new Matrix(new double[][]{{70,50,3},{62,53,99},{4,66,32},{54,88,91},{12,32,13}}));

        // I have created a matrix here, and the other input is created inside the POJO by default.
        DataSource<Matrix> matrixDataSet1 = env.fromElements(new Matrix(new double[][]{{70, 50, 3, 9, 10}, {62, 53, 99, 7, 8}, {4, 66, 32, 5, 6}, {54, 88, 91, 3, 4}, {12, 32, 13, 1, 2}}));


        DataSet<Matrix> matrixDataSet2 = env.fromElements(new Matrix());


        // I use broadCast variable to load the two matrices into the same mapper
        DataSet<Matrix> resultMatrixMultiplyDataSet1 = matrixDataSet1.map(new RichMapFunction<Matrix, Matrix>() {

            Collection<Matrix> broadCastedInputMatrix;

            @Override
            public void open(Configuration parameters) throws Exception {
                broadCastedInputMatrix = getRuntimeContext().getBroadcastVariable("matrixDataSet2");
            }

            @Override
            public Matrix map(Matrix value) throws Exception {

                Iterator<Matrix> matrixIterator = broadCastedInputMatrix.iterator();
                Matrix m1 = broadCastedInputMatrix.iterator().next();
                Matrix m2 = value;

                return MatrixFlink.multiplyMatrices(m1, m2);

            }
        }).withBroadcastSet(matrixDataSet2, "matrixDataSet2");


        // change the multiplication with investing the input dataset to check if the trace is the same
        DataSet<Matrix> resultMatrixMultiplyDataSet2 = matrixDataSet2.map(new RichMapFunction<Matrix, Matrix>() {

            Collection<Matrix> broadCastedInputMatrix;

            @Override
            public void open(Configuration parameters) throws Exception {
                broadCastedInputMatrix = getRuntimeContext().getBroadcastVariable("matrixDataSet1");
            }

            @Override
            public Matrix map(Matrix value) throws Exception {

                Iterator<Matrix> matrixIterator = broadCastedInputMatrix.iterator();
                Matrix m1 = broadCastedInputMatrix.iterator().next();
                Matrix m2 = value;

                return MatrixFlink.multiplyMatrices(m1, m2);

            }
        }).withBroadcastSet(matrixDataSet1, "matrixDataSet1");


        // I write here the result matrix with its trace value, check @override toString method in the POJO
        resultMatrixMultiplyDataSet1.writeAsText("/tmp/flink/matrix/Result1");
        resultMatrixMultiplyDataSet2.writeAsText("/tmp/flink/matrix/Result2");
        env.execute();
    }


    public static Matrix multiplyMatrices(Matrix m1, Matrix m2) {

        int m1rows = m1.getMatrix().length;
        int m1cols = m1.getMatrix()[0].length;
        int m2rows = m2.getMatrix().length;
        int m2cols = m2.getMatrix()[0].length;

        Matrix result = new Matrix(m1rows, m2cols);

        if (m1cols != m2rows) {
            throw new IllegalArgumentException("matrices don't match: " + m1cols + " != " + m2rows);
        } else {
            for (int i = 0; i < m1rows; i++) {
                for (int j = 0; j < m2cols; j++) {
                    for (int k = 0; k < m1cols; k++) {
                        result.getMatrix()[i][j] += m1.getMatrix()[i][k] * m2.getMatrix()[k][j];

                    }
                }
            }
        }
        return result;
    }

}
