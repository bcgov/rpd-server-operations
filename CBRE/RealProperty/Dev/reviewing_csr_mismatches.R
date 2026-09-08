rmpct_raw <- raw_data
test_rmpct <- rmpct_raw |>
  filter(rmpct_ls_id == "A111286-L2680")

rm_raw <- raw_data
test_rm <- rm_raw |>
  filter(rm_bl_id == "B0067021")

test_rm_pkg_space <- test_rm |>
  filter(rm_rm_id == "594395")

test2_rmpct <- rmpct_raw |>
  filter(rmpct_ls_id == "A118884-L3148")

test2_rm <- rm_raw |>
  filter(rm_bl_id == "B0058453")

test2_rm_pkg_space <- test2_rm |>
  filter(rm_rm_id == "22")
