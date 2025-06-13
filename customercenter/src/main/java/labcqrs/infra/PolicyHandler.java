package labcqrs.infra;

import labcqrs.config.kafka.KafkaProcessor;
import labcqrs.domain.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Optional;

import javax.transaction.Transactional;

@Service
@Transactional
public class PolicyHandler {

    @Autowired
    MyPageRepository myPageRepository;

    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverOrderPlaced_CreateMyPage(@Payload OrderPlaced event) {
        if (!event.validate()) return;

        System.out.println("##### listener OrderPlaced : " + event.toJson());

        MyPage myPage = new MyPage();
        myPage.setOrderId(event.getId());
        myPage.setProductId(event.getProductId());
        myPage.setOrderStatus(event.getStatus());

        myPageRepository.save(myPage);
    }

    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverDeliveryStarted_UpdateDeliveryStatus(@Payload DeliveryStarted event) {
        if (!event.validate()) return;

        System.out.println("##### listener DeliveryStarted : " + event.toJson());

        Optional<MyPage> optional = myPageRepository.findByOrderId(event.getOrderId());
        if (optional.isPresent()) {
            MyPage myPage = optional.get();
            myPage.setDeliveryStatus(event.getStatus());
            myPageRepository.save(myPage);
        }
    }
}
