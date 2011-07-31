class Object
  def returning(value)
    yield(value)
    value
  end 
end

class Array
  def to_hash
    inject({}) { |m, e| m[e[0]] = e[1]; m }
  end
end
