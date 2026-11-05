variable "region" {
  type    = string
  default = "ap-southeast-2"
}
variable "alert_emails" {
  type = list(string)
}