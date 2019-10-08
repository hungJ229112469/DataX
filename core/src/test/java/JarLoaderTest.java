import com.alibaba.datax.core.util.container.JarLoader;

/**
 * @author HL
 * @date 2019/9/25 14:29
 */
public class JarLoaderTest {

    public static void main(String[] args) throws Exception {
        String transformerPath = "D:\\Develop\\datax\\local_storage\\transformer\\sfzjh";
        JarLoader jarLoader = new JarLoader(new String[]{transformerPath});
        String className = "com.alibaba.datax.transformer.IDCardTrasformer";
        Class<?> transformerClass = jarLoader.loadClass(className);
        Object transformer = transformerClass.newInstance();
    }
}
