package ai.turbochain.ipex.consumer;

import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import ai.turbochain.ipex.constant.BooleanEnum;
import ai.turbochain.ipex.entity.Coin;
import ai.turbochain.ipex.service.CoinService;
import ai.turbochain.ipex.service.MemberWalletService;
import ai.turbochain.ipex.service.WithdrawRecordService;
import ai.turbochain.ipex.util.MessageResult;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;

@Component
public class FinanceConsumer {
	private Logger logger = LoggerFactory.getLogger(FinanceConsumer.class);
	@Autowired
	private CoinService coinService;
	@Autowired
	private MemberWalletService walletService;
	@Autowired
	private RestTemplate restTemplate;
	@Autowired
	private WithdrawRecordService withdrawRecordService;

	/**
	 * 处理充值消息，key值为币种的名称（注意是全称，如Bitcoin）
	 *
	 * @param record
	 */
	@KafkaListener(topics = { "deposit" })
	public void handleDeposit(ConsumerRecord<String, String> record) {
		logger.info("topic={},key={},value={}", record.topic(), record.key(), record.value());
		if (StringUtils.isEmpty(record.value())) {
			return;
		}
		JSONObject json = JSON.parseObject(record.value());
		if (json == null) {
			return;
		}
		BigDecimal amount = json.getBigDecimal("amount");
		String txid = json.getString("txid");
		String address = json.getString("address");
		Coin coin = coinService.findOne(record.key());
		logger.info("coin={}", coin);

		if (coin != null && walletService.findDeposit(address, txid) == null) {
			MessageResult mr = walletService.recharge(coin, address, amount, txid);
			logger.info("wallet recharge result:{}", mr);
		}
	}

	/**
	 * 处理提交请求,调用钱包rpc，自动转账
	 *
	 * @param record
	 */
	@KafkaListener(topics = { "withdraw" })
	public void handleWithdraw(ConsumerRecord<String, String> record) {
		logger.info("topic={},key={},value={}", record.topic(), record.key(), record.value());
		if (StringUtils.isEmpty(record.value())) {
			return;
		}
		JSONObject json = JSON.parseObject(record.value());
		Long withdrawId = json.getLong("withdrawId");
		try {
			Coin coin = coinService.findByUnit(record.key());
			// BigDecimal minerFee = coin.getMinerFee();
			String serviceName = "";
			String url = "";
			MessageResult result = MessageResult.success();
			logger.info("coin = {}", coin.toString());
			if (coin != null && coin.getCanAutoWithdraw() == BooleanEnum.IS_TRUE
					&& coin.getEnableRpc() == BooleanEnum.IS_TRUE) {
				if (coin.getIsToken().isIs() == true && coin.getChainName() != null) {
					// 远程RPC服务URL,后缀为币种单位
					serviceName = "SERVICE-RPC-" + coin.getChainName().toUpperCase();
					url = "http://" + serviceName
							+ "/rpc/withdraw?username={1}&address={2}&amount={3}&contractAddress={4}&decimals={5}&coinName={6}";
					result = restTemplate.getForObject(url, MessageResult.class, "U" + json.getLong("uid"),
							json.getString("address"), json.getBigDecimal("arriveAmount"), coin.getTokenAddress(),
							coin.getDecimals(), coin.getUnit());
				} else {
					serviceName = "SERVICE-RPC-" + coin.getUnit().toUpperCase();
					url = "http://" + serviceName + "/rpc/withdraw?username={1}&address={2}&amount={3}";
					result = restTemplate.getForObject(url, MessageResult.class, "U" + json.getLong("uid"),
							json.getString("address"), json.getBigDecimal("arriveAmount"));
				}
				logger.info("=========================rpc 结束================================");
				logger.info("result = {}", result);
				if (result.getCode() == 0 && result.getData() != null) {
					logger.info("====================== 处理成功,data为txid更新业务 ==================================");
					// 处理成功,data为txid，更新业务订单
					String txid = (String) result.getData();
					withdrawRecordService.withdrawSuccess(withdrawId, txid);
				} else {
					logger.info("====================== 自动转账失败，转为人工处理 ==================================");
					// 自动转账失败，转为人工处理
					withdrawRecordService.autoWithdrawFail(withdrawId);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("auto withdraw failed,error={}", e.getMessage());
			logger.info("====================== 自动转账失败，转为人工处理 ==================================");
//            自动转账失败，转为人工处理
			withdrawRecordService.autoWithdrawFail(withdrawId);
		}
	}

	/**
	 * HardId提现
	 *
	 * @param record
	 */
	@KafkaListener(topics = { "hardId-withdraw" })
	public void handIdWithdraw(ConsumerRecord<String, String> record) {
		logger.info("topic={},key={},value={}", record.topic(), record.key(), record.value());
		if (StringUtils.isEmpty(record.value())) {
			return;
		}
		JSONObject json = JSON.parseObject(record.value());
		Long withdrawId = json.getLong("withdrawId");
		try {
			Coin coin = coinService.findByUnit(record.key());
			String serviceName = "";
			String url = "";
			MessageResult result = MessageResult.success();
			logger.info("coin = {}", coin.toString());
			if (coin != null && coin.getCanAutoWithdraw() == BooleanEnum.IS_TRUE
					&& coin.getEnableRpc() == BooleanEnum.IS_TRUE) {
				if (coin.getIsToken().isIs() == true && coin.getChainName() != null) {
					// 远程RPC服务URL,后缀为币种单位
					serviceName = "SERVICE-RPC-" + coin.getChainName().toUpperCase();
					url = "http://" + serviceName
							+ "/rpc/withdraw?username={1}&address={2}&amount={3}&contractAddress={4}&decimals={5}&coinName={6}";
					result = restTemplate.getForObject(url, MessageResult.class, "U" + json.getLong("uid"),
							json.getString("address"), json.getBigDecimal("arriveAmount"), coin.getTokenAddress(),
							coin.getDecimals(), coin.getUnit());
				} else {
					serviceName = "SERVICE-RPC-" + coin.getUnit().toUpperCase();
					url = "http://" + serviceName + "/rpc/withdraw?username={1}&address={2}&amount={3}";
					result = restTemplate.getForObject(url, MessageResult.class, "U" + json.getLong("uid"),
							json.getString("address"), json.getBigDecimal("arriveAmount"));
				}
				logger.info("=========================rpc 结束================================");
				logger.info("result = {}", result);
				if (result.getCode() == 0 && result.getData() != null) {
					logger.info("====================== 处理成功,data为txid更新业务 ==================================");
					// 处理成功,data为txid，更新业务订单
					String txid = (String) result.getData();
					withdrawRecordService.withdrawSuccess(withdrawId, txid);
				} else {
					logger.info("====================== 提现失败 ==================================");
					withdrawRecordService.withdrawFailOfHardId(withdrawId);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
