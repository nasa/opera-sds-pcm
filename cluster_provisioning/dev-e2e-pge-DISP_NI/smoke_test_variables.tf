# k, m, number of k-sets and PGE simulation mode of the DISP-NI smoke test's historical walk, passed to
# run_smoke_test.sh through smoke_test_inputs.config. variables.tf is shared with the dev venue, so these
# live here.
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

# true runs the walk without the PGE: each SCIFLO_L3_DISP_NI job writes the outputs the PGE would for its inputs
variable "disp_ni_smoke_pge_sim_mode" {
  type    = bool
  default = false
}
