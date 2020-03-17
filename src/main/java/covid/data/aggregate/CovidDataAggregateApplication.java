package covid.data.aggregate;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import static org.springframework.web.bind.annotation.RequestMethod.POST;

@SpringBootApplication
public class CovidDataAggregateApplication {

	String[] HEADERS = {"province", "country", "lastUpdate", "confirmed", "deaths", "recovered"};

	@Autowired
	StreamBridge streamBridge;

	@Autowired
	InteractiveQueryService interactiveQueryService;

	public static void main(String[] args) {
		SpringApplication.run(CovidDataAggregateApplication.class, args);
	}

	@RequestMapping(path = "/upload", method = POST, consumes = "*/*")
	@ResponseStatus(HttpStatus.ACCEPTED)
	public void upload(@RequestParam("data-file") MultipartFile multipartFile) throws IOException {
		String name = multipartFile.getOriginalFilename();
		byte[] bytes = multipartFile.getBytes();

		Iterable<CSVRecord> records = CSVFormat.DEFAULT
				.withHeader(HEADERS)
				.withFirstRecordAsHeader()
				.parse(new StringReader(new String(bytes)));

		final List<CovidData> collect = StreamSupport.stream(records.spliterator(), false)
				.map(csvRecord -> {
					CovidData covidData = new CovidData();
					covidData.setProvince(csvRecord.get(0));
					covidData.setCountry(csvRecord.get(1));
					if (covidData.getCountry().equalsIgnoreCase("mainland china")) {
						covidData.setCountry("China");
					}
					covidData.setLastUpdate(csvRecord.get(2));
					covidData.setConfirmed(StringUtils.isEmpty(csvRecord.get(3)) ? 0 : Integer.parseInt(csvRecord.get(3)));
					covidData.setDeaths(StringUtils.isEmpty(csvRecord.get(4)) ? 0 : Integer.parseInt(csvRecord.get(4)));
					covidData.setRecovered(StringUtils.isEmpty(csvRecord.get(5)) ? 0 : Integer.parseInt(csvRecord.get(5)));
					covidData.setOrigFileName(name);
					return covidData;
				})
				.collect(Collectors.toList());

		collect.forEach(t -> streamBridge.send("covid-raw-out-0", t));
	}

	@Bean
	public Function<CovidData, Message<CovidData>> transform() {
		return covidData -> {
			covidData.setUuid(UUID.randomUUID());
			return MessageBuilder.withPayload(covidData)
					.setHeader(KafkaHeaders.MESSAGE_KEY, covidData.getCountry().replace(" ", "-").getBytes())
					.build();
		};
	}

	@Bean
	public StoreBuilder myStore() {
		return Stores.keyValueStoreBuilder(
				Stores.persistentKeyValueStore("de-duplicate-store"), Serdes.String(),
				new JsonSerde<>(CovidData.class));
	}

	@Bean
	public Function<KStream<String, CovidData>, KStream<String, CovidDataAggregate>> process() {
		return input ->
				input.transform(new TransformerSupplier<String, CovidData, KeyValue<String, CovidData>>() {
					@Override
					public Transformer<String, CovidData, KeyValue<String, CovidData>> get() {
						return new Transformer<String, CovidData, KeyValue<String, CovidData>>() {

							private KeyValueStore<String, CovidData> deDuplicateStore;

							@Override
							public void init(ProcessorContext context) {
								this.deDuplicateStore = (KeyValueStore) context.getStateStore("de-duplicate-store");
							}

							@Override
							public KeyValue<String, CovidData> transform(String key, CovidData value) {
								final CovidData covidData = deDuplicateStore.get(value.getProvince() + value.getCountry() + value.getLastUpdate() + value.getOrigFileName());
								if (covidData == null) {
									deDuplicateStore.put(value.getProvince() + value.getCountry() + value.getLastUpdate() + value.getOrigFileName(), value);
									return KeyValue.pair(key, value);
								}
								else {
									//If we already see this record, disallow forwarding this record downstream.
									return null;
								}
							}

							@Override
							public void close() {

							}
						};
					}
				}, "de-duplicate-store")
						.groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(CovidData.class)))
						.aggregate(() -> new CovidDataAggregate(),
								(aggKey, newValue, aggValue) -> {

									if (newValue.getProvince() != null) {
										if (StringUtils.isEmpty(aggValue.getOrigFileName()) || aggValue.getOrigFileName().equals(newValue.getOrigFileName())) {
											aggValue.setConfirmed(aggValue.getConfirmed() + newValue.getConfirmed());
											aggValue.setDeaths(aggValue.getDeaths() + newValue.getDeaths());
											aggValue.setRecovered(aggValue.getRecovered() + newValue.getRecovered());
											aggValue.setOrigFileName(newValue.getOrigFileName());
											aggValue.setLastUpdate(newValue.getLastUpdate());
										}
										else {
											simpleAggregate(newValue, aggValue);
										}
									}
									else {
										simpleAggregate(newValue, aggValue);
									}

									return aggValue;
								}, Materialized.<String, CovidDataAggregate, KeyValueStore<Bytes, byte[]>>as("aggregate-count-by-country")
										.withKeySerde(Serdes.String()).withValueSerde(new JsonSerde<>(CovidDataAggregate.class)))
						.toStream();
	}

	private void simpleAggregate(CovidData newValue, CovidDataAggregate aggValue) {
		aggValue.setConfirmed(newValue.getConfirmed());
		aggValue.setDeaths(newValue.getDeaths());
		aggValue.setRecovered(newValue.getRecovered());
		aggValue.setOrigFileName(newValue.getOrigFileName());
		aggValue.setLastUpdate(newValue.getLastUpdate());
	}

	@RestController
	public class FooController {

		@RequestMapping("/counts/{country}")
		public CovidDataAggregateByCountry song(@PathVariable("country") String country) {
			final ReadOnlyKeyValueStore<String, CovidDataAggregate> covidAggreateStore =
					interactiveQueryService.getQueryableStore("aggregate-count-by-country", QueryableStoreTypes.keyValueStore());
			final KeyValueIterator<String, CovidDataAggregate> all = covidAggreateStore.all();
			//Sanitize the key for potential case issues
			String[] key = new String[1];
			all.forEachRemaining(covidDataAggregateKeyValue -> {
				if (covidDataAggregateKeyValue.key.toLowerCase().contains(country.toLowerCase())) {
					key[0] = covidDataAggregateKeyValue.key;
				}
			});
			final CovidDataAggregate covidDataAggregate = covidAggreateStore.get(key[0]);
			if (covidDataAggregate == null) {
				throw new IllegalArgumentException("No data found for:  " + key[0]);
			}
			CovidDataAggregateByCountry covidDataAggregateByCountry = new CovidDataAggregateByCountry();
			covidDataAggregateByCountry.setCountry(country);
			covidDataAggregateByCountry.setConfirmed(covidDataAggregate.getConfirmed());
			covidDataAggregateByCountry.setDeaths(covidDataAggregate.getDeaths());
			covidDataAggregateByCountry.setRecovered(covidDataAggregate.getRecovered());
			return covidDataAggregateByCountry;
		}
	}
}
