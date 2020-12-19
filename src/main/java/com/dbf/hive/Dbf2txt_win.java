package com.dbf.hive;

import com.linuxense.javadbf.DBFReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * java -jar 20201215_dbf_hive-1.0-SNAPSHOT.jar com.dbf.hive.Dbf2txt
 */


public class Dbf2txt_win {
    public static int i = 0;

    public static void main(String[] args) {
        Dbf2txt.dbf2txt();
        System.out.println("dbf文件总数--------------" + i);
       /* try {
            Dbf2txt.txt2hdfs();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("上传成功--------------" + i);*/
    }

    public static void dbf2txt() {

        String paths = "/root/test/dbf2txt/dbf/";
        File[] files = new File(paths).listFiles();
        for (File f : files) {
            if (!f.getName().contains(".dbf")) {
                continue;
            }
            i++;
            InputStream fis = null;
            OutputStreamWriter fos = null;
            try {
                String filePaths = paths + f.getName();
                String configPath = URLDecoder.decode(filePaths, "GBK");
                // 读取文件的输入流
                fis = new FileInputStream(configPath);
                // 根据输入流初始化一个DBFReader实例，用来读取DBF文件信息
                DBFReader reader = new DBFReader(fis);
                reader.setCharactersetName("GBK");
                // 调用DBFReader对实例方法得到path文件中字段的个数
                int fieldsCount = reader.getFieldCount();
                //获取输出文件流
                String ss = URLDecoder.decode("/root/test/dbf2txt/txt/" + f.getName().split("\\.")[0] + ".txt", "UTF-8");
                //fos = new FileOutputStream(ss);
                fos = new OutputStreamWriter(new FileOutputStream(ss), "UTF-8");
                Object[] rowValues;
                // 一条条取出path文件中记录xa
                while ((rowValues = reader.nextRecord()) != null) {
                    StringBuffer sb = new StringBuffer();
                    for (int i = 0; i < rowValues.length; i++) {
                        String res = "";
                        if (rowValues[i] == null) {
                            sb.append(res).append(",");
                            continue;
                        }
                        switch (rowValues[i].toString()) {
                            case "0E-11":
                                res = "0";
                                break;
                            default:
                                res = rowValues[i].toString();
                                break;
                        }
                        sb.append(res).append(",");
                    }
                    String line = sb.substring(0, sb.toString().length() - 1);
                    fos.write(line);
                    fos.write("\n");
                    fos.flush();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    fis.close();
                    fos.close();
                } catch (Exception e) {
                }
            }
        }
    }

   /* public static void txt2hdfs() throws IOException, InterruptedException {
        String paths = "/root/test/dbf2txt/txt/";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hdfsCluster");
        // 设置认证参数
        conf.set("hadoop_security_authentication_tbds_username", "root");
        conf.set("hadoop_security_authentication_tbds_secureid", "xPuMVjpi7NuTtE6ffG275OUPjQGxwWvBDA4a");
        conf.set("hadoop_security_authentication_tbds_securekey", "1bfJBFTxQg9184GpPlu5gP1r6PcJRLkF");
        // 将配置强制设置为程序运行时的环境配置
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(null);
        FileSystem fs = FileSystem.get(URI.create("hdfs://hdfsCluster:8080"), conf);
        File[] files = new File(paths).listFiles();
        for (File f : files) {
            fs.copyFromLocalFile(new Path(paths + f.getName()), new Path("/root/test/db/"));
        }
        fs.close();
    }*/


}

