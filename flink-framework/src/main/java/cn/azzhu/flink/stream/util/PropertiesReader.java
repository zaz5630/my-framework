package cn.azzhu.flink.stream.util;

/**
 * 配置读取类
 * @author azzhu
 * @since 2021-02-18 23:06
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesReader {

    private String project_root = "";
    private File file = null;


    public PropertiesReader(String filePath) {
        //构造时获取到项目的物理根目录
        try {
            file = new File(filePath);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public String getProperties(String key) {
        InputStream fis = null;
        try {
            Properties prop = new Properties();
            fis = new FileInputStream(getAbsolutePath());

            prop.load(fis);

            return prop.getProperty(key);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (Exception e) {
            }
        }

        return "";
    }

    public void setProperties(String key, String value) throws Exception {
        Properties prop = new Properties();


        FileOutputStream outputFile = null;
        InputStream fis = null;
        try {
            //输入流和输出流要分开处理， 放一起会造成写入时覆盖以前的属性
            fis = new FileInputStream(getAbsolutePath());
            //先载入已经有的属性文件
            prop.load(fis);

            //追加新的属性
            prop.setProperty(key, value);

            //写入属性
            outputFile = new FileOutputStream(getAbsolutePath());
            prop.store(outputFile, "");

            outputFile.flush();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (Exception e) {
            }
            try {
                if (outputFile != null) {
                    outputFile.close();
                }
            } catch (Exception e) {
            }
        }
    }


    public String getAbsolutePath() {
        try {
            return file.getAbsolutePath();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}
