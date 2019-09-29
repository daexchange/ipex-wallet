package ai.turbochain.ipex.consumer;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.web.client.RestTemplate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import ai.turbochain.ipex.constant.ActivityRewardType;
import ai.turbochain.ipex.constant.BooleanEnum;
import ai.turbochain.ipex.constant.RewardRecordType;
import ai.turbochain.ipex.constant.TransactionType;
import ai.turbochain.ipex.entity.Coin;
import ai.turbochain.ipex.entity.Member;
import ai.turbochain.ipex.entity.MemberLegalCurrencyWallet;
import ai.turbochain.ipex.entity.MemberTransaction;
import ai.turbochain.ipex.entity.MemberWallet;
import ai.turbochain.ipex.entity.RewardActivitySetting;
import ai.turbochain.ipex.entity.RewardRecord;
import ai.turbochain.ipex.es.ESUtils;
import ai.turbochain.ipex.service.CoinService;
import ai.turbochain.ipex.service.MemberLegalCurrencyWalletService;
import ai.turbochain.ipex.service.MemberService;
import ai.turbochain.ipex.service.MemberTransactionService;
import ai.turbochain.ipex.service.MemberWalletService;
import ai.turbochain.ipex.service.RewardActivitySettingService;
import ai.turbochain.ipex.service.RewardRecordService;
import ai.turbochain.ipex.util.BigDecimalUtils;
import ai.turbochain.ipex.util.MessageResult;

@Component
public class MemberConsumer {
	private Logger logger = LoggerFactory.getLogger(MemberConsumer.class);
	@Autowired
	private RestTemplate restTemplate;
	@Autowired
	private CoinService coinService;
	@Autowired
	private MemberWalletService memberWalletService;
	@Autowired
	private MemberLegalCurrencyWalletService memberLegalCurrencyWalletService;
	@Autowired
	private RewardActivitySettingService rewardActivitySettingService;
	@Autowired
	private MemberService memberService;
	@Autowired
	private RewardRecordService rewardRecordService;
	@Autowired
	private MemberTransactionService memberTransactionService;
	@Autowired
	private ESUtils esUtils;
	
	@Autowired
	private ExecutorService executorService;

	/**
	 * 重置用户钱包地址
	 * 
	 * @param record
	 */
	@KafkaListener(topics = { "reset-member-address" })
	public void resetAddress(ConsumerRecord<String, String> record) {
		String content = record.value();
		JSONObject json = JSON.parseObject(content);
		Coin coin = coinService.findByUnit(record.key());
		Assert.notNull(coin, "coin null");
		if (coin.getEnableRpc() == BooleanEnum.IS_TRUE) {
			MemberWallet memberWallet = memberWalletService.findByCoinUnitAndMemberId(record.key(),
					json.getLong("uid"));
			Assert.notNull(memberWallet, "wallet null");
			// String account = "U" + json.getLong("uid")+ GeneratorUtil.getNonceString(4);
			String account = "U" + json.getLong("uid");
			// 远程RPC服务URL,后缀为币种单位
			String serviceName = "SERVICE-RPC-" + coin.getUnit();
			try {
				String url = "http://" + serviceName + "/rpc/address/{account}";
				ResponseEntity<MessageResult> result = restTemplate.getForEntity(url, MessageResult.class, account);
				logger.info("remote call:service={},result={}", serviceName, result);
				if (result.getStatusCode().value() == 200) {
					MessageResult mr = result.getBody();
					if (mr.getCode() == 0) {
						String address = mr.getData().toString();
						memberWallet.setAddress(address);
					}
				}
			} catch (Exception e) {
				logger.error("call {} failed,error={}", serviceName, e.getMessage());
			}
			memberWalletService.save(memberWallet);
		}

	}

	/**
	 * 客户注册消息
	 * 
	 * @param content
	 */
	@KafkaListener(topics = { "member-register" })
	public void handle(String content) {
		logger.info("handle member-register,data={}", content);
		if (StringUtils.isEmpty(content)) {
			return;
		}
		JSONObject json = JSON.parseObject(content);
		if (json == null) {
			return;
		}
		
		afterRegister(json);
	}
	
	/**
	 * 注册成功后的操作
	 */
	public void afterRegister(JSONObject json){
        executorService.execute(new Runnable() {
            public void run() {
            	registerCoin(json);
            }
        });
    }
	
	public void registerCoin(JSONObject json ) {
		// 获取所有支持的币种
		List<Coin> coins = coinService.findAll();
		for (Coin coin : coins) {
			logger.info("memberId:{},unit:{}", json.getLong("uid"), coin.getUnit());
			MemberWallet wallet = new MemberWallet();
			wallet.setCoin(coin);
			wallet.setMemberId(json.getLong("uid"));
			wallet.setBalance(new BigDecimal(0));
			wallet.setFrozenBalance(new BigDecimal(0));
			wallet.setAddress("");
            if(coin.getEnableRpc() == BooleanEnum.IS_TRUE) {
                String account = "U" + json.getLong("uid");
                //远程RPC服务URL,后缀为币种单位
                String serviceName = "SERVICE-RPC-" + coin.getUnit();
                try{
                    String url = "http://" + serviceName + "/rpc/address/{account}";
                    ResponseEntity<MessageResult> result = restTemplate.getForEntity(url, MessageResult.class, account);
                    logger.info("remote call:service={},result={}", serviceName, result);
                    if (result.getStatusCode().value() == 200) {
                        MessageResult mr = result.getBody();
                        logger.info("mr={}", mr);
                        if (mr.getCode() == 0) {
                            //返回地址成功，调用持久化
                            String address = (String) mr.getData();
                            wallet.setAddress(address);
                        }
                    }
                }
                catch (Exception e){
                    logger.error("call {} failed,error={}",serviceName,e.getMessage());
                    wallet.setAddress("");
                }
            } else {
                wallet.setAddress("");
            }
            
			// 保存
			memberWalletService.save(wallet);

			MemberLegalCurrencyWallet memberLegalCurrencyWallet = new MemberLegalCurrencyWallet();

			memberLegalCurrencyWallet.setCoin(coin);
			memberLegalCurrencyWallet.setMemberId(json.getLong("uid"));
			memberLegalCurrencyWallet.setBalance(new BigDecimal(0));
			memberLegalCurrencyWallet.setFrozenBalance(new BigDecimal(0));

			memberLegalCurrencyWalletService.save(memberLegalCurrencyWallet);
		}
		// 注册活动奖励
		RewardActivitySetting rewardActivitySetting = rewardActivitySettingService
				.findByType(ActivityRewardType.REGISTER);
		if (rewardActivitySetting != null) {
			MemberWallet memberWallet = memberWalletService.findByCoinAndMemberId(rewardActivitySetting.getCoin(),
					json.getLong("uid"));
			if (memberWallet == null) {
				return;
			}
			BigDecimal amount3 = JSONObject.parseObject(rewardActivitySetting.getInfo()).getBigDecimal("amount");
			memberWallet.setBalance(BigDecimalUtils.add(memberWallet.getBalance(), amount3));
			memberWalletService.save(memberWallet);
			Member member = memberService.findOne(json.getLong("uid"));
			RewardRecord rewardRecord3 = new RewardRecord();
			rewardRecord3.setAmount(amount3);
			rewardRecord3.setCoin(rewardActivitySetting.getCoin());
			rewardRecord3.setMember(member);
			rewardRecord3.setRemark(rewardActivitySetting.getType().getCnName());
			rewardRecord3.setType(RewardRecordType.ACTIVITY);
			rewardRecordService.save(rewardRecord3);
			MemberTransaction memberTransaction = new MemberTransaction();
			memberTransaction.setFee(BigDecimal.ZERO);
			memberTransaction.setAmount(amount3);
			memberTransaction.setSymbol(rewardActivitySetting.getCoin().getUnit());
			memberTransaction.setType(TransactionType.ACTIVITY_AWARD);
			memberTransaction.setMemberId(member.getId());
			memberTransaction.setDiscountFee("0");
			memberTransaction.setRealFee("0");
			memberTransaction = memberTransactionService.save(memberTransaction);
		}
	}
	
}
