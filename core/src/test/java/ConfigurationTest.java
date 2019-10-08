import com.alibaba.datax.common.util.Configuration;

/**
 * @author HL
 * @date 2019/9/27 15:14
 */
public class ConfigurationTest {

    public static void main(String[] args) {
        Configuration from = Configuration.from("{}");
        Object root = from.set("root.index.type", 123);
        System.out.println(from.toString());

    }
}
