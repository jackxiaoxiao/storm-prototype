package cn.egova.storm_python;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by tcb on 2017/2/17.
 * java 调用getRuntime()方式向python脚本传递参数。
 */
public class javaToPython {
    public static void main(String[] args){
        args = new String[]{"/usr/bin/python","/home/tan_hadoop/apache-storm-0.10.0/testdata/jpython.py","a","b", "c","d" };
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(args);
        } catch (IOException e) {
            e.printStackTrace();
        }
        InputStream inputStream = process.getInputStream();
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        String line;
        try {
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }
            inputStream.close();
            System.out.println(process.waitFor());//0 代表成功执行！1表示还没执行完python脚本。
            System.out.print("python.py is end!");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
