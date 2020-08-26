export df_PROJECT=df-project-2020-07-17-srashid
export NONCE=2020-07-17-srashid
export tenant_1=tenant-1-$NONCE
export tenant_2=tenant-2-$NONCE
export df_PROJECT_NUMBER=`gcloud projects describe  $df_PROJECT --format="value(projectNumber)"`
export df_COMPUTE_SVC_ACCOUNT=$df_PROJECT_NUMBER-compute@developer.gserviceaccount.com
