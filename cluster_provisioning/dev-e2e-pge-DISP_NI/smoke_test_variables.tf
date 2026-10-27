# k and m of the DISP-NI smoke test's historical walk, passed to run_smoke_test.sh through
# smoke_test_inputs.config. variables.tf is shared with the dev venue, so these live here.
variable "disp_ni_smoke_k" {
  type    = number
  default = 5
}

variable "disp_ni_smoke_m" {
  type    = number
  default = 2
}
