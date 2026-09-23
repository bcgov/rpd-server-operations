# Copy this to .Renviron and fill in your value
# dev = targets Windfarm | prod = targets Dynamo
ETL_ENV = dev
RENV_CONFIG_NAMESPACES_CHECK = FALSE
RENV_CONFIG_CONNECT_TIMEOUT=5
RENV_CONFIG_CONNECT_RETRY=0
# On internet-restricted machines, uncomment and set path to local package repo:
# RENV_CONFIG_REPOS_OVERRIDE=file:///E:/Projects/packagerepo

# maybe try this
# options(repos = NULL)
