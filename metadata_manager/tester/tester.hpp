template <class T>
class CExStack
{
private:
    std::unique_ptr<T[]> m_values;
    size_t m_index;

public:
    CEzStack();
    CEzStack(const CEzStack& stack);
    CEzStack(CEzStack&& stack);

    void push(const T& value);
    T& pop();

};

