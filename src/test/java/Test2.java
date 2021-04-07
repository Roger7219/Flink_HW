/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/7 8:52
 */
public class Test2<T> {
    T a;
    
    public <K> void foo(T aa){
    
    }
    
    public static void main(String[] args) {
        Test2<String> test2 = new Test2<>();
        
        test2.foo("a");
    }
}
