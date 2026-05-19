package app

type AppSecret struct {
	Kafka KafkaSecret `mapstructure:"kafka"`
}

type KafkaSecret struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}
