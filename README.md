     __  __               ___  __                                                               ______  ____    ______     
    /\ \/\ \            /'___\/\ \                   /'\_/`\  __                               /\  _  \/\  _`\ /\__  _\    
    \ \ \/'/'     __   /\ \__/\ \ \/'\      __      /\      \/\_\  _ __   _ __   ___   _ __    \ \ \L\ \ \ \L\ \/_/\ \/    
     \ \ , <    /'__`\ \ \ ,__\\ \ , <    /'__`\    \ \ \__\ \/\ \/\`'__\/\`'__\/ __`\/\`'__\   \ \  __ \ \ ,__/  \ \ \    
      \ \ \\`\ /\ \L\.\_\ \ \_/ \ \ \\`\ /\ \L\.\_   \ \ \_/\ \ \ \ \ \/ \ \ \//\ \L\ \ \ \/     \ \ \/\ \ \ \/    \_\ \__ 
       \ \_\ \_\ \__/.\_\\ \_\   \ \_\ \_\ \__/.\_\   \ \_\\ \_\ \_\ \_\  \ \_\\ \____/\ \_\      \ \_\ \_\ \_\    /\_____\
        \/_/\/_/\/__/\/_/ \/_/    \/_/\/_/\/__/\/_/    \/_/ \/_/\/_/\/_/   \/_/ \/___/  \/_/       \/_/\/_/\/_/    \/_____/
                                                                                
A Java Api to enable mirroring topic(s) from one kafka broker to another.

### The problem

When you have an application running active on one data center and another in passive mode on a different data center, 
with clients on both DCs that want to subscribe the same information published for both DCs.