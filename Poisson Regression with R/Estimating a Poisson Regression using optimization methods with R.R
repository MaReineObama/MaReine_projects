install.packages("tidyverse")
require(tidyverse)

x = rdunif(1500,10,a=1)

y = rdunif(1500,5,a=1)


#Define the likelihood_poisson function 
likelihood_poisson = function(param,dataset){
  
  b0 = param[1]
  b1 = param[2]
  
  x = dataset[[1]]
  y = dataset[[2]
  
  return(-sum(y*(b0 + b1*x) - exp(b0+b1*x) - lfactorial(y)))
  
}

dataset = list(x,y)

#The paramaters will be optimized over 2 values
optimPoisson <- optim(par=c(1.0,1.0),fn=likelihood_poisson,dataset=list(x,y),method = "L-BFGS-B")

optimPoisson$par

#we set beta 0 and beta 1 final
b0_final = optimPoisson$par[1]
b1_final = optimPoisson$par[2]

#we compute lambda
lambda = mean(exp(b0_final + b1_final*x))

print(lambda)
