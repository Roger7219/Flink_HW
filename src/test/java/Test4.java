import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/4/10 9:34
 */
public class Test4 {
    public static void main(String[] args) {
        LocalDateTime todayTime = LocalDateTime.ofEpochSecond(System.currentTimeMillis() / 1000, 0, ZoneOffset.ofHours(8));
        LocalDateTime tomorrowTime = LocalDateTime.of(todayTime.toLocalDate().plusDays(1), LocalTime.of(0, 0, 1));
        System.out.println(tomorrowTime);
    }
}
