int interval = 6; // Check hours from now
Date now = new Date();
Calendar c = Calendar.getInstance();
c.setTime(now);
c.add(Calendar.HOUR, -interval);
Date horizon = c.getTime();

boolean result = false;

for (int i = 0; i < ctx.results[0].aggregations.history_hosts.buckets.size(); i++) {
Date last_date = new Date( (long) ctx.results[0].aggregations.history_hosts.buckets[i].max_time_processed.value );
if ( last_date.before(horizon) ) {
result = true;
}
}
return result;
