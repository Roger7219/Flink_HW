import org.apache.flink.util.MathUtils;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/1 15:30
 */
public class Test1 {
    public static void main(String[] args) {
//        Integer a = 0;
        String a = "奇数";
//        Integer b = 1;
        String b = "偶数";
    
        System.out.println(MathUtils.murmurHash(a.hashCode())%128);
        System.out.println(MathUtils.murmurHash(b.hashCode())%128);
    }
}
