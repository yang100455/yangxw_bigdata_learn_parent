package cn.lizzy.learn.req;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName : UserBehavior
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/4/8 20:37
 * @DESCRIPTION :
 * @since JDK 1.8
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
