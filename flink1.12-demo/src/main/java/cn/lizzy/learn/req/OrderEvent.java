package cn.lizzy.learn.req;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName : OrderEvent
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/4/12 19:52
 * @DESCRIPTION :
 * @since JDK 1.8
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {

    private Long orderId;
    private String eventType;
    private String txId;
    private Long eventTime;
}
