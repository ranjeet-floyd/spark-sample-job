/*
 * To change this license header, choose License Headers in Project Properties.
 * and open the template in the editor.
 */
package spark.sample.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;

/**
 *
 * @author ranjeet
 */
public class MainProgram implements Serializable {

    final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {

        String filename = "s3n://yourbucketname/*" ;
        System.out.println("filename : " + filename);
        MainProgram wholeTextFiles = new MainProgram();
        wholeTextFiles.run(filename);
        System.out.println("Done !!");
    }

    private void run(String filename) {
        System.out.println("Run spark clsuter");
        SparkConf sparkConf = new SparkConf().setAppName(MainProgram.class.getName());
//        sparkConf.setMaster("spark://192.168.2.152:7077");
        sparkConf.set("spark.cores.max", "6");
        sparkConf.set("spark.driver.cores", "2");
        sparkConf.set("spark.driver.memory", "1g");
        sparkConf.set("spark.executor.memory", "1g");
        //don't set master if you want to run from spark rest api
        sparkConf.setMaster("local[4]");//.setMaster("spark://192.168.2.152:7077");
               
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
        javaSparkContext.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "your aws AccessKeyId ");
        javaSparkContext.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "your aws SecretAccessKey");

        JavaRDD<String> fileNameContentsRDD = javaSparkContext.textFile(filename, 5);

        long output = fileNameContentsRDD.count();
        System.out.println("Output lenght : " + output);
        System.out.println("************Done******************");

    }

}
