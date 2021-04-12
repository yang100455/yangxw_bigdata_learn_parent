package cn.lizzy.learn.req;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName : AdsClickLog
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/4/9 10:33
 * @DESCRIPTION :
 * @since JDK 1.8
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdsClickLog {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;

}
