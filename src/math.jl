
function allocate(amount::Int, ratios::Vector{<:Int})
  total = sum(ratios)
  shares = map(ratio -> Int(floor(amount * ratio / total)), ratios)
  remainder = amount - sum(shares)
  # sortperm gives index to interate in sort-order, without sorting
  for i in sortperm(shares; rev=true)
    if remainder == 0
      break
    end
    shares[i] += 1
    remainder -= 1
  end
  return shares
end

function string_pointed(value::Int, places::Int)
  str = string(value)
  if startswith(str, "-")
    str = "-" * lpad(replace(str, "-" => ""), places+1, '0')
  else
    str = lpad(str, places+1, '0')
  end
  len = length(str)
  ndx = len - places
  return SubString(str, 1:ndx) * "." * SubString(str, (ndx+1):len)
end

function dtoi(value::String)::Int
  return parse(Int, replace(value, r"[\.\$,]+" => ""))
end

function itod(value::Int)::String
  return "\$" * string_pointed(value, 2)
end

function qtoi(value::Float64)::Int
  return Int(floor(value * 1000));
end

function itoq(value::Int)::Float64
  return parse(Float64, string_pointed(value, 3))
end

