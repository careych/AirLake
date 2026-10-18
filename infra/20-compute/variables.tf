variable "region" {
  type    = string
  default = "ap-southeast-2"
}

variable "kafka_instance_type" {
  type    = string
  default = "c7i-flex.large"
}

variable "superset_instance_type" {
  type    = string
  default = "c7i-flex.large"
}

variable "spark_instance_type" {
  type    = string
  default = "m7i-flex.large"
}