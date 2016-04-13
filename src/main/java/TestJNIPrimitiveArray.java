import java.util.*;
import java.io.*;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.IOException;
import javax.imageio.ImageIO;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;



public class TestJNIPrimitiveArray {
   static {
      System.load("/home/make/finaljc/mnist/mnistCUDNN.so"); // myjni.dll (Windows) or libmyjni.so (Unixes)
   }
 
   // Declare a native method sumAndAverage() that receives an int[] and
   //  return a double[2] array with [0] as sum and [1] as average
   private native double[] imgDNN(int[] numbers);


	public static Integer[] toConvertInteger(int[] ids) {

 	 Integer[] newArray = new Integer[ids.length];
     for (int i = 0; i < ids.length; i++) {
       newArray[i] = Integer.valueOf(ids[i]);
     }
   	 return newArray;
	}

	public static double imgSend(int[] nums) {
        double[] tt = new TestJNIPrimitiveArray().imgDNN(nums);
        return (tt[1] + tt[0]);
    }

	 private static int[] convertTo2DUsingGetRGB(BufferedImage image) {
      int width = image.getWidth();
      int height = image.getHeight();      
      int[] res = new int[height*width];

      for (int row = 0; row < height; row++) {
         for (int col = 0; col < width; col++) {

            int p = image.getRGB(col,row);
            int a = (p>>24)&0xff;
            int r = (p>>16)&0xff;
            int g = (p>>8)&0xff;
            int b = p&0xff;

            int avg = (r+g+b)/3;

            p = (a<<24) | (avg<<16) | (avg<<8) | avg;
			res[row*width + col] = avg;

         }
      }

      return res;
   }

 
   // Test Driver
   public static void main(String args[]) {

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
 
	    int[] numbers = {22, 33, 33};

		BufferedImage bufImg1 = null;
		BufferedImage bufImg2 = null;

		try{
        	bufImg1 = ImageIO.read(new File("digit4.jpg"));
        	bufImg2 = ImageIO.read(new File("mnist_1.png"));
		} catch(IOException e) {
		}
   

        long startTime = System.nanoTime();
        //int[] img1 = {1,2,3,4,5,6};//convertTo2DUsingGetRGB(bufImg1);
	//	int[] img2 = {1,2,3,4,5,6};//convertTo2DUsingGetRGB(bufImg2);		

		int[] img1 = convertTo2DUsingGetRGB(bufImg1);
		int[] img2 = convertTo2DUsingGetRGB(bufImg2);		



        long endTime = System.nanoTime();
		String imStr1;
/*
		double[] d_img1 = new double[img1.length];
		double[] d_img2 = new double[img2.length];


		for(int i = 0; i < img1.length; i++) {
			d_img1[i] = (double)img1[i];
			d_img2[i] = (double)img2[i];
		}

		Vector img1Vect = Vectors.dense(d_img1);
        Vector img2Vect = Vectors.dense(d_img2);
*/
  
        ArrayList<Integer[]> imgList =  new ArrayList<Integer[]>();



        imgList.add(toConvertInteger(img1));
        imgList.add(toConvertInteger(img2));

        JavaRDD<Integer[]> imgsRDD = sc.parallelize(imgList);

        JavaRDD<Integer> ress = imgsRDD.map( new Function<Integer[], Integer>() {
				@Override
                public Integer call(Integer[] img) {

					int[] cImg = new int[img.length];

					for(int ctr = 0; ctr < img.length; ctr++) {
						cImg[ctr] = img[ctr].intValue(); // returns int value
					}
				
				    double t = imgSend(cImg);

					Integer d = (int)t;
                    return d;
                }
            });

		ress.collect().forEach(x -> System.out.println(x));
        //System.out.println("Lines with a: " + numAs + "fdsfds");

   /*   int[] numbers = {22, 33, 33};
      double[] results = new TestJNIPrimitiveArray().imgDNN(numbers);
  
	    String logFile = "/home/make/spark-1.6.1/README.md"; // Should be some file on your system
       JavaRDD<String> logData = sc.textFile(logFile).cache();
   
        long numAs = logData.filter( new Function<String, Boolean>() {
                public Boolean call(String s) {
                    int[] a = {1,2,3};
                    double t = imgSend(a);
                    return s.contains("a");
                }
            }).count();

        System.out.println("Lines with a: " + numAs + "fdsfds");	  */
	}
}

