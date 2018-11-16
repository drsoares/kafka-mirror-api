     __  __               ___  __                                                            
    /\ \/\ \            /'___\/\ \                   /'\_/`\  __                             
    \ \ \/'/'     __   /\ \__/\ \ \/'\      __      /\      \/\_\  _ __   _ __   ___   _ __  
     \ \ , <    /'__`\ \ \ ,__\\ \ , <    /'__`\    \ \ \__\ \/\ \/\`'__\/\`'__\/ __`\/\`'__\
      \ \ \\`\ /\ \L\.\_\ \ \_/ \ \ \\`\ /\ \L\.\_   \ \ \_/\ \ \ \ \ \/ \ \ \//\ \L\ \ \ \/ 
       \ \_\ \_\ \__/.\_\\ \_\   \ \_\ \_\ \__/.\_\   \ \_\\ \_\ \_\ \_\  \ \_\\ \____/\ \_\ 
        \/_/\/_/\/__/\/_/ \/_/    \/_/\/_/\/__/\/_/    \/_/ \/_/\/_/\/_/   \/_/ \/___/  \/_/ 
                                                                                
A Java Api to enable mirroring topic(s) from one kafka broker to another.

### The problem

When you have an application running active on one data center and another in passive mode on a different data center, 
with clients on both DCs that want to subscribe the same information published for both DCs.