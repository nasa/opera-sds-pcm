# k, m and number of k-sets of the DISP-NI smoke test's historical walk, passed to run_smoke_test.sh
# through smoke_test_inputs.config. variables.tf is shared with the dev venue, so these live here.
variable "disp_ni_smoke_k" {
  type    = number
  default = 5
}

variable "disp_ni_smoke_m" {
  type    = number
  default = 3
}

variable "disp_ni_smoke_k_sets" {
  type    = number
  default = 5
}
